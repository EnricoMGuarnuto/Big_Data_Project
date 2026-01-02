import os
import time
from typing import Optional
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# =========================
# Env / Config
# =========================
KAFKA_BROKER            = os.getenv("KAFKA_BROKER", "kafka:9092")
# topics (inputs)
TOPIC_SHELF_STATE       = os.getenv("TOPIC_SHELF_STATE", "shelf_state")            # compacted
TOPIC_SHELF_BATCH_STATE = os.getenv("TOPIC_SHELF_BATCH_STATE", "shelf_batch_state")# compacted (key shelf_id::batch_code)
TOPIC_SHELF_POLICIES    = os.getenv("TOPIC_SHELF_POLICIES", "shelf_policies")      # compacted
# topics (outputs)
TOPIC_ALERTS            = os.getenv("TOPIC_ALERTS", "alerts")                      # append-only
TOPIC_SHELF_RESTOCK     = os.getenv("TOPIC_SHELF_RESTOCK", "shelf_restock_plan")   # compacted (key shelf_id)

# Delta Lake paths (optional mirrors)
DELTA_ROOT              = os.getenv("DELTA_ROOT", "/delta")
DL_SHELF_STATE_PATH     = os.getenv("DL_SHELF_STATE_PATH", f"{DELTA_ROOT}/cleansed/shelf_state")
DL_SHELF_BATCH_PATH     = os.getenv("DL_SHELF_BATCH_PATH", f"{DELTA_ROOT}/cleansed/shelf_batch_state")
DL_ALERTS_PATH          = os.getenv("DL_ALERTS_PATH", f"{DELTA_ROOT}/ops/alerts")
DL_RESTOCK_PATH         = os.getenv("DL_RESTOCK_PATH", f"{DELTA_ROOT}/ops/shelf_restock_plan")

# Optional: load maximum_stock from Postgres for better suggestions
JDBC_PG_URL      = os.getenv("JDBC_PG_URL")
JDBC_PG_USER     = os.getenv("JDBC_PG_USER")
JDBC_PG_PASSWORD = os.getenv("JDBC_PG_PASSWORD")
LOAD_MAX_FROM_PG = os.getenv("LOAD_MAX_FROM_PG", "1") in ("1","true","True")
MAX_STOCK_TABLE  = os.getenv("MAX_STOCK_TABLE", "ref.store_inventory_snapshot")
POLICIES_PG_TABLE = os.getenv("POLICIES_PG_TABLE", "ref.shelf_restock_policy")
BOOTSTRAP_POLICIES_FROM_PG = os.getenv("BOOTSTRAP_POLICIES_FROM_PG", "1") in ("1","true","True")

# Engine params
STARTING_OFFSETS        = os.getenv("STARTING_OFFSETS", "earliest")
NEAR_EXPIRY_DAYS        = int(os.getenv("NEAR_EXPIRY_DAYS", "5"))        # expiry alerts window
ALERTS_COOLDOWN_MINUTES = int(os.getenv("ALERTS_COOLDOWN_MINUTES", "60"))
DEFAULT_TARGET_PCT      = float(os.getenv("DEFAULT_TARGET_PCT", "80.0")) # target refill level if none provided
CHECKPOINT_ROOT         = os.getenv("CHECKPOINT_ROOT", f"{DELTA_ROOT}/_checkpoints/shelf_alert_engine")
CKP_SHELF_STATE         = os.getenv("CKP_SHELF_STATE", f"{CHECKPOINT_ROOT}/shelf_state")
CKP_FOREACH             = os.getenv("CKP_FOREACH", f"{CHECKPOINT_ROOT}/foreach")
DL_TXN_APP_ID           = os.getenv("DL_TXN_APP_ID", "shelf_alert_engine_delta_ops_alerts")
DELTA_WRITE_MAX_RETRIES = int(os.getenv("DELTA_WRITE_MAX_RETRIES", "8"))
DELTA_WRITE_RETRY_BASE_S = float(os.getenv("DELTA_WRITE_RETRY_BASE_S", "1.0"))

# =========================
# Spark Session (Delta)
# =========================
spark = (
    SparkSession.builder.appName("Shelf_Alert_Engine")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def _delta_field_types(path: str) -> dict:
    if not DeltaTable.isDeltaTable(spark, path):
        return {}
    try:
        schema = spark.read.format("delta").load(path).schema
        return {f.name: f.dataType for f in schema.fields}
    except Exception as e:
        print(f"[delta-schema] warn reading schema from {path}: {e}")
        return {}

def _is_delta_conflict(err: Exception) -> bool:
    name = err.__class__.__name__
    msg = str(err)
    return (
        "Concurrent" in name
        or "DELTA_CONCURRENT" in msg
        or "MetadataChangedException" in name
        or "DELTA_METADATA_CHANGED" in msg
        or "ProtocolChangedException" in name
    )

def _write_alerts_delta_with_retries(df, batch_id: int) -> None:
    last = None
    for attempt in range(1, DELTA_WRITE_MAX_RETRIES + 1):
        try:
            (df.write.format("delta")
             .mode("append")
             .option("mergeSchema", "true")
             .option("txnAppId", DL_TXN_APP_ID)
             .option("txnVersion", str(batch_id))
             .save(DL_ALERTS_PATH))
            return
        except Exception as e:
            last = e
            if not _is_delta_conflict(e) or attempt == DELTA_WRITE_MAX_RETRIES:
                raise
            sleep_s = DELTA_WRITE_RETRY_BASE_S * attempt
            print(f"[delta-retry] alerts append conflict ({attempt}/{DELTA_WRITE_MAX_RETRIES}) -> sleep {sleep_s}s: {e}")
            time.sleep(sleep_s)
    raise last  # pragma: no cover

def kafka_topic_exists(topic_name: str) -> bool:
    """Check topic presence via Kafka AdminClient (avoids UnknownTopic errors on read)."""
    try:
        jvm = spark._jvm
        props = jvm.java.util.Properties()
        props.setProperty("bootstrap.servers", KAFKA_BROKER)
        admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
        try:
            names_future = admin.listTopics().names()
            topic_names = names_future.get()
            return topic_names.contains(topic_name)
        finally:
            admin.close()
    except Exception as e:
        print(f"[kafka-topic-check] warn for {topic_name}: {e}")
        return False

# =========================
# Schemas (Kafka values are JSON strings)
# =========================
schema_shelf_state = T.StructType([
    T.StructField("shelf_id", T.StringType()),
    T.StructField("current_stock", T.IntegerType()),
    T.StructField("item_weight", T.DoubleType()),  # optional naming in your publisher (alias of shelf_weight)
    T.StructField("shelf_weight", T.DoubleType()),
    T.StructField("last_update_ts", T.TimestampType()),
])

schema_policy = T.StructType([
    T.StructField("policy_id", T.StringType()),
    T.StructField("shelf_id", T.StringType()),
    T.StructField("item_category", T.StringType()),
    T.StructField("item_subcategory", T.StringType()),
    T.StructField("threshold_pct", T.DoubleType()),
    T.StructField("min_qty", T.IntegerType()),
    T.StructField("severity", T.StringType()),
    T.StructField("active", T.BooleanType()),
    T.StructField("notes", T.StringType()),
    T.StructField("updated_at", T.TimestampType()),
])

schema_batch = T.StructType([
    T.StructField("shelf_id", T.StringType()),
    T.StructField("batch_code", T.StringType()),
    T.StructField("received_date", T.DateType()),
    T.StructField("expiry_date", T.DateType()),
    T.StructField("batch_quantity_store", T.IntegerType()),
    T.StructField("last_update_ts", T.TimestampType()),
])

# =========================
# Helpers: load latest policies snapshot (static)
# =========================
def bootstrap_policies_if_missing():
    """
    Se il topic shelf_policies è vuoto e BOOTSTRAP_POLICIES_FROM_PG=1,
    legge le policy da Postgres e le pubblica su Kafka (compacted).
    Key = shelf_id (oppure '__GLOBAL__' se NULL).
    """
    if not BOOTSTRAP_POLICIES_FROM_PG:
        print("[policies-bootstrap] BOOTSTRAP_POLICIES_FROM_PG=0 -> skip.")
        return

    # 1) Controlla se il topic ha già dati
    if kafka_topic_exists(TOPIC_SHELF_POLICIES):
        try:
            existing = (
                spark.read.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKER)
                .option("subscribe", TOPIC_SHELF_POLICIES)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load()
            )
            if existing.limit(1).count() > 0:
                print(f"[policies-bootstrap] Topic {TOPIC_SHELF_POLICIES} non è vuoto -> skip.")
                return
        except Exception as e:
            print(f"[policies-bootstrap] warning durante il check del topic: {e}")
    else:
        print(f"[policies-bootstrap] Topic {TOPIC_SHELF_POLICIES} non esiste ancora -> bootstrap necessario.")

    # 2) Parametri JDBC
    if not (JDBC_PG_URL and JDBC_PG_USER and JDBC_PG_PASSWORD):
        print("[policies-bootstrap] JDBC params mancanti -> skip.")
        return

    # 3) Leggi Postgres (ultima policy per shelf_id, includendo la globale shelf_id NULL)
    last_err = None
    for attempt in range(1, 6):
        try:
            base = (
                spark.read.format("jdbc")
                .option("url", JDBC_PG_URL)
                .option("user", JDBC_PG_USER)
                .option("password", JDBC_PG_PASSWORD)
                .option("dbtable", f"(SELECT policy_id, shelf_id, item_category, item_subcategory, "
                                   f"threshold_pct, min_qty, severity, active, updated_at "
                                   f"FROM {POLICIES_PG_TABLE}) t")
                .load()
            )
            break
        except Exception as e:
            last_err = e
            print(f"[policies-bootstrap] read failed ({attempt}/5): {e}")
            time.sleep(3)
    else:
        raise RuntimeError("Unable to read policies from Postgres") from last_err

    from pyspark.sql.window import Window
    w = Window.partitionBy("shelf_id").orderBy(F.col("updated_at").desc_nulls_last())
    latest = (
        base.withColumn("rn", F.row_number().over(w))
            .where("rn = 1 AND active = true")
            .drop("rn")
    )

    # 4) Pubblica su Kafka compacted (key = shelf_id oppure __GLOBAL__)
    to_publish = (
        latest
        .withColumn("k", F.coalesce(F.col("shelf_id"), F.lit("__GLOBAL__")))
        .withColumn(
            "value_json",
            F.to_json(
                F.struct(
                    "policy_id", "shelf_id", "item_category", "item_subcategory",
                    "threshold_pct", "min_qty", "severity", "active", "updated_at"
                )
            )
        )
        .select(
            F.col("k").cast("string").alias("key"),
            F.col("value_json").cast("string").alias("value")
        )
    )

    to_publish.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", TOPIC_SHELF_POLICIES) \
        .save()

    print(f"[policies-bootstrap] Published initial policies to topic {TOPIC_SHELF_POLICIES}.")

def load_policies_latest():
    """Load latest active policies from Kafka (compacted topic) once at startup.
       We keep the most recent record per shelf_id, and also detect a global default (null shelf_id).
    """
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_SHELF_POLICIES)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        df.select(
            F.col("key").cast("string").alias("policy_key"),
            F.col("value").cast("string").alias("value_str"),
            "partition", "offset", "timestamp"
        )
        .withColumn("value_json", F.from_json("value_str", schema_policy))
        .select(
            F.coalesce(F.col("value_json.shelf_id"), F.lit(None)).alias("shelf_id"),
            F.col("value_json.threshold_pct").alias("threshold_pct"),
            F.col("value_json.min_qty").alias("min_qty"),
            F.col("value_json.severity").alias("severity"),
            F.col("value_json.active").alias("active"),
            "partition", "offset", "timestamp"
        )
        .filter(F.col("active") == True)
    )

    w = Window.partitionBy("shelf_id").orderBy(F.col("offset").desc())
    latest = (
        parsed.withColumn("rn", F.row_number().over(w))
              .where("rn = 1")
              .drop("rn","partition","offset")
              .cache()
    )
    return latest


def load_max_stock_latest() -> Optional["pyspark.sql.dataframe.DataFrame"]:
    if not (LOAD_MAX_FROM_PG and JDBC_PG_URL and JDBC_PG_USER and JDBC_PG_PASSWORD):
        return None

    last_err = None
    for attempt in range(1, 6):
        try:
            base = (
                spark.read.format("jdbc")
                .option("url", JDBC_PG_URL)
                .option("user", JDBC_PG_USER)
                .option("password", JDBC_PG_PASSWORD)
                .option("dbtable", f"(SELECT shelf_id, maximum_stock, snapshot_ts FROM {MAX_STOCK_TABLE}) t")
                .load()
            )
            w = Window.partitionBy("shelf_id").orderBy(F.col("snapshot_ts").desc())
            latest = base.withColumn("rn", F.row_number().over(w)).where("rn=1").select("shelf_id","maximum_stock")
            return latest
        except Exception as e:
            last_err = e
            print(f"[max_stock] read failed ({attempt}/5): {e}")
            time.sleep(3)
    print("[max_stock] giving up, will fallback to heuristic targets")
    return None

bootstrap_policies_if_missing()
POLICIES_LATEST = load_policies_latest()
MAX_STOCK_LATEST = load_max_stock_latest()

# =========================
# Streaming source: shelf_state (driver stream)
# =========================
raw_state = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_SHELF_STATE)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .load()
)

state_events = (
    raw_state
    .select(F.col("value").cast("string").alias("value_str"))
    .withColumn("json", F.from_json("value_str", schema_shelf_state))
    .select("json.*")
    .filter(F.col("shelf_id").isNotNull())
)

# =========================
# ForeachBatch: compute alerts + restock plan for shelves touched in this micro-batch
# =========================

def foreach_batch_alerts(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    shelves = batch_df.select("shelf_id","current_stock").distinct().cache()

    # Simulated "now" derived from event-time in this micro-batch
    sim_now = batch_df.select(F.max("last_update_ts").alias("sim_now")).collect()[0]["sim_now"]
    if sim_now is None:
        # fallback: if producer didn't send last_update_ts (should not happen)
        sim_now = batch_df.select(F.current_timestamp().alias("sim_now")).collect()[0]["sim_now"]

    sim_now_ts = F.lit(sim_now).cast("timestamp")
    sim_now_date = F.to_date(sim_now_ts)
    cutoff_ts = sim_now_ts - F.expr(f"INTERVAL {ALERTS_COOLDOWN_MINUTES} MINUTES")

    # Join with policies: prefer per-shelf else fallback to global (NULL shelf)
    pol = POLICIES_LATEST.alias("p")
    joined = (
        shelves.alias("s")
        .join(pol.filter(F.col("shelf_id").isNotNull()).alias("ps"), F.col("s.shelf_id") == F.col("ps.shelf_id"), "left")
        .join(pol.filter(F.col("shelf_id").isNull()).alias("pg"))  # cross join single-row global
    )

    # choose specific over global
    with_policy = (
        joined.select(
            F.col("s.shelf_id").alias("shelf_id"),
            F.col("s.current_stock").alias("current_stock"),
            F.coalesce(F.col("ps.threshold_pct"), F.col("pg.threshold_pct")).alias("threshold_pct"),
            F.coalesce(F.col("ps.min_qty"), F.col("pg.min_qty")).alias("min_qty"),
            F.coalesce(F.col("ps.severity"), F.col("pg.severity"), F.lit("medium")).alias("severity")
        )
    )

    # Enrich with maximum_stock if available
    if MAX_STOCK_LATEST is not None:
        with_policy = with_policy.join(MAX_STOCK_LATEST, on="shelf_id", how="left")
    else:
        with_policy = with_policy.withColumn("maximum_stock", F.lit(None).cast("int"))

    # Compute stock % and suggested qty
    enriched = (
        with_policy
        .withColumn("threshold_pct", F.coalesce(F.col("threshold_pct"), F.lit(30.0)))
        .withColumn("min_qty", F.coalesce(F.col("min_qty"), F.lit(1)))
        .withColumn("target_pct", F.lit(DEFAULT_TARGET_PCT))
        .withColumn("stock_pct", F.when(F.col("maximum_stock").isNotNull() & (F.col("maximum_stock") > 0),
                                         F.col("current_stock") * 100.0 / F.col("maximum_stock")).otherwise(F.lit(None)))
        .withColumn("target_qty", F.when(F.col("maximum_stock").isNotNull(),
                                         F.ceil(F.col("maximum_stock") * F.col("target_pct")/100.0)).otherwise(F.lit(None)))
        .withColumn("suggested_qty", F.when(F.col("target_qty").isNotNull(),
                                            F.greatest(F.lit(0), F.col("target_qty") - F.col("current_stock"))).otherwise(
                                            F.greatest(F.lit(0), F.col("min_qty") - F.col("current_stock"))))
    )

    # Refill alerts: trigger if stock_pct<threshold_pct or current_stock<min_qty
    refill_needed = (
        enriched.filter(
            ( (F.col("stock_pct").isNotNull()) & (F.col("stock_pct") < F.col("threshold_pct")) ) |
            (F.col("current_stock") < F.col("min_qty"))
        )
        .withColumn("event_type", F.lit("refill_request"))
        .withColumn("location", F.lit("store"))
    )

    # Near-expiry alerts (look at soonest batch for each shelf)
    try:
        shelf_batches = spark.read.format("delta").load(DL_SHELF_BATCH_PATH)
        soonest = (
            shelf_batches
            .select("shelf_id","expiry_date","batch_quantity_store")
            .groupBy("shelf_id")
            .agg(
                F.min("expiry_date").alias("min_expiry"),
                F.sum("batch_quantity_store").alias("qty_on_batches")
            )
        )
        near_expiry = (
            soonest.join(shelves, on="shelf_id", how="inner")
            .withColumn("days_to_expiry", F.datediff(F.col("min_expiry"), sim_now_date))
            .filter(F.col("min_expiry").isNotNull() & (F.col("days_to_expiry") <= F.lit(NEAR_EXPIRY_DAYS)) & (F.col("days_to_expiry") >= 0))
            .withColumn("event_type", F.lit("near_expiry"))
            .withColumn("location", F.lit("store"))
            .withColumn("severity", F.lit("high"))
            .withColumn("suggested_qty", F.lit(0))
        )
    except Exception as e:
        print(f"[near_expiry] warning: {e}")
        near_expiry = spark.createDataFrame([], schema=refill_needed.select("shelf_id","event_type","location","severity","suggested_qty").schema)\
                          .withColumn("current_stock", F.lit(None).cast("int"))\
                          .withColumn("min_expiry", F.lit(None).cast("date"))\
                          .withColumn("days_to_expiry", F.lit(None).cast("int"))

    # Compose alerts (refill + near_expiry)
    alerts_cols = [
        "event_type","shelf_id","location","severity",
        "current_stock","min_qty","threshold_pct","stock_pct","suggested_qty"
    ]

    refill_alerts = (
        refill_needed.select(
            "event_type", "shelf_id", "location", "severity",
            "current_stock", "min_qty", "threshold_pct", "stock_pct", "suggested_qty"
        )
    )

    expiry_alerts = (
        near_expiry.select(
            "event_type","shelf_id","location","severity",
            F.col("qty_on_batches").alias("current_stock"),
            F.lit(None).cast("int").alias("min_qty"),
            F.lit(None).cast("double").alias("threshold_pct"),
            F.lit(None).cast("double").alias("stock_pct"),
            F.col("suggested_qty")
        )
    )

    alerts_df = (
        refill_alerts.unionByName(expiry_alerts, allowMissingColumns=True)
        .withColumn("created_at", sim_now_ts)
    )

    if ALERTS_COOLDOWN_MINUTES > 0 and DeltaTable.isDeltaTable(spark, DL_ALERTS_PATH):
        try:
            recent = (
                spark.read.format("delta").load(DL_ALERTS_PATH)
                .where(F.col("created_at") >= cutoff_ts)
                .select("event_type", "shelf_id", "location")
                .dropDuplicates()
            )
            alerts_df = alerts_df.join(recent, on=["event_type", "shelf_id", "location"], how="left_anti")
        except Exception as e:
            print(f"[alerts-dedupe] warn: {e}")

    # Write alerts → Kafka (append-only)
    if alerts_df.rdd.isEmpty() is False:
        alerts_out = alerts_df.withColumn("value",
            F.to_json(F.struct(*alerts_cols, "created_at"))
        ).select(F.lit(None).cast("string").alias("key"), "value")

        alerts_out.write.format("kafka")\
            .option("kafka.bootstrap.servers", KAFKA_BROKER)\
            .option("topic", TOPIC_ALERTS)\
            .save()

        # Also persist to Delta (optional)
        # Normalize types to match existing Delta schema (prevents merge field conflicts).
        alerts_delta_types = _delta_field_types(DL_ALERTS_PATH)
        t = lambda name, default: alerts_delta_types.get(name, default)
        alerts_to_delta = alerts_df.select(
            F.col("event_type").cast(t("event_type", T.StringType())).alias("event_type"),
            F.col("shelf_id").cast(t("shelf_id", T.StringType())).alias("shelf_id"),
            F.col("location").cast(t("location", T.StringType())).alias("location"),
            F.col("severity").cast(t("severity", T.StringType())).alias("severity"),
            F.col("current_stock").cast(t("current_stock", T.IntegerType())).alias("current_stock"),
            F.col("min_qty").cast(t("min_qty", T.IntegerType())).alias("min_qty"),
            F.col("threshold_pct").cast(t("threshold_pct", T.DoubleType())).alias("threshold_pct"),
            F.col("stock_pct").cast(t("stock_pct", T.DoubleType())).alias("stock_pct"),
            F.col("suggested_qty").cast(t("suggested_qty", T.IntegerType())).alias("suggested_qty"),
            F.col("created_at").cast(t("created_at", T.TimestampType())).alias("created_at"),
        )

        _write_alerts_delta_with_retries(alerts_to_delta, batch_id)

    # Build shelf_restock_plan (only from refill alerts with positive suggested qty)
    plans = (
        refill_needed.filter(F.col("suggested_qty") > 0)
        .select(
            F.col("shelf_id"),
            F.col("suggested_qty"),
            F.lit("pending").alias("status"),
            sim_now_ts.alias("created_at"),
            sim_now_ts.alias("updated_at"),
            F.expr("uuid()").alias("plan_id")
        )
    )

    if plans.rdd.isEmpty() is False:
        # Kafka compacted (key = shelf_id)
        plans_k = plans.withColumn("value",
            F.to_json(F.struct("plan_id","shelf_id","suggested_qty","status","created_at","updated_at"))
        ).select(F.col("shelf_id").alias("key"), F.col("value"))

        plans_k.write.format("kafka")\
            .option("kafka.bootstrap.servers", KAFKA_BROKER)\
            .option("topic", TOPIC_SHELF_RESTOCK)\
            .save()

        # Delta mirror (upsert by shelf_id -> keep only latest plan per shelf)
        if DeltaTable.isDeltaTable(spark, DL_RESTOCK_PATH):
            tgt = DeltaTable.forPath(spark, DL_RESTOCK_PATH)
            tgt.alias("t").merge(
                plans.alias("s"),
                "t.shelf_id = s.shelf_id"
            ).whenMatchedUpdate(set={
                "plan_id": F.col("s.plan_id"),
                "suggested_qty": F.col("s.suggested_qty"),
                "status": F.col("s.status"),
                "updated_at": F.col("s.updated_at")
            }).whenNotMatchedInsert(values={
                "plan_id": F.col("s.plan_id"),
                "shelf_id": F.col("s.shelf_id"),
                "suggested_qty": F.col("s.suggested_qty"),
                "status": F.col("s.status"),
                "created_at": F.col("s.created_at"),
                "updated_at": F.col("s.updated_at")
            }).execute()
        else:
            plans.write.format("delta").mode("overwrite").save(DL_RESTOCK_PATH)

# =========================
# Start streaming
# =========================
query = (
    state_events.writeStream
    .foreachBatch(foreach_batch_alerts)
    .option("checkpointLocation", CKP_FOREACH)
    .start()
)

query.awaitTermination()
