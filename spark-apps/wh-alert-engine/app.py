import os
import time
from typing import Optional
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from simulated_time.clock import get_simulated_now  # simulated clock
from datetime import timedelta


# =========================
# Env / Config
# =========================
KAFKA_BROKER             = os.getenv("KAFKA_BROKER", "kafka:9092")
# inputs (compacted)
TOPIC_WH_STATE           = os.getenv("TOPIC_WH_STATE", "wh_state")
TOPIC_WH_BATCH_STATE     = os.getenv("TOPIC_WH_BATCH_STATE", "wh_batch_state")
TOPIC_WH_POLICIES        = os.getenv("TOPIC_WH_POLICIES", "wh_policies")
# outputs
TOPIC_ALERTS             = os.getenv("TOPIC_ALERTS", "alerts")                 # append-only
TOPIC_WH_SUPPLIER_PLAN   = os.getenv("TOPIC_WH_SUPPLIER_PLAN", "wh_supplier_plan")  # compacted (key shelf_id)

# Delta Lake mirrors (optional)
DELTA_ROOT               = os.getenv("DELTA_ROOT", "/delta")
DL_WH_STATE_PATH         = os.getenv("DL_WH_STATE_PATH", f"{DELTA_ROOT}/cleansed/wh_state")
DL_WH_BATCH_PATH         = os.getenv("DL_WH_BATCH_PATH", f"{DELTA_ROOT}/cleansed/wh_batch_state")
DL_ALERTS_PATH           = os.getenv("DL_ALERTS_PATH", f"{DELTA_ROOT}/ops/alerts")
DL_SUPPLIER_PLAN_PATH    = os.getenv("DL_SUPPLIER_PLAN_PATH", f"{DELTA_ROOT}/ops/wh_supplier_plan")
DL_TXN_APP_ID            = os.getenv("DL_TXN_APP_ID", "wh_alert_engine_delta_ops_alerts")
DELTA_WRITE_MAX_RETRIES  = int(os.getenv("DELTA_WRITE_MAX_RETRIES", "8"))
DELTA_WRITE_RETRY_BASE_S = float(os.getenv("DELTA_WRITE_RETRY_BASE_S", "1.0"))

# Optional Postgres bootstrap for policies
JDBC_PG_URL              = os.getenv("JDBC_PG_URL")
JDBC_PG_USER             = os.getenv("JDBC_PG_USER")
JDBC_PG_PASSWORD         = os.getenv("JDBC_PG_PASSWORD")
WH_POLICIES_PG_TABLE     = os.getenv("WH_POLICIES_PG_TABLE", "ref.wh_restock_policy")
BOOTSTRAP_WH_POLICIES_FROM_PG = os.getenv("BOOTSTRAP_WH_POLICIES_FROM_PG", "1") in ("1","true","True")

# Engine params
STARTING_OFFSETS         = os.getenv("STARTING_OFFSETS", "earliest")
FAIL_ON_DATA_LOSS        = os.getenv("FAIL_ON_DATA_LOSS", "0") in ("1", "true", "True")
NEAR_EXPIRY_DAYS         = int(os.getenv("NEAR_EXPIRY_DAYS", "10"))  # warehouse horizon (default 10)
DEFAULT_MULTIPLIER       = int(os.getenv("DEFAULT_MULTIPLIER", "1"))  # multiples of standard_batch_size
CHECKPOINT_ROOT          = os.getenv("CHECKPOINT_ROOT", f"{DELTA_ROOT}/_checkpoints/wh_alert_engine")
CKP_FOREACH              = os.getenv("CKP_FOREACH", f"{CHECKPOINT_ROOT}/foreach")
ALERTS_COOLDOWN_MINUTES  = int(os.getenv("ALERTS_COOLDOWN_MINUTES", "60"))
WH_SUPPLIER_PLAN_ENABLED = os.getenv("WH_SUPPLIER_PLAN_ENABLED", "1") in ("1", "true", "True")

# =========================
# Spark Session
# =========================
spark = (
    SparkSession.builder.appName("WH_Alert_Engine")
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

def _ensure_delta_columns(path: str, expected_types: dict) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        return
    existing = _delta_field_types(path)
    missing = {name: dtype for name, dtype in expected_types.items() if name not in existing}
    if not missing:
        return
    ddl = ", ".join([f"{name} {dtype.simpleString()}" for name, dtype in missing.items()])
    try:
        spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS ({ddl})")
    except Exception as e:
        print(f"[delta-schema] warn adding columns to {path}: {e}")

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
            print(f"[delta-retry] wh_alert_engine append conflict ({attempt}/{DELTA_WRITE_MAX_RETRIES}) -> sleep {sleep_s}s: {e}")
            time.sleep(sleep_s)
    raise last  # pragma: no cover

def _merge_supplier_plans_with_retries(plans_df) -> None:
    last = None
    for attempt in range(1, DELTA_WRITE_MAX_RETRIES + 1):
        try:
            _ensure_delta_columns(
                DL_SUPPLIER_PLAN_PATH,
                {f.name: f.dataType for f in schema_wh_supplier_plan.fields},
            )
            tgt = DeltaTable.forPath(spark, DL_SUPPLIER_PLAN_PATH)
            (tgt.alias("t").merge(
                plans_df.alias("s"),
                "t.shelf_id = s.shelf_id"
            ).whenMatchedUpdate(set={
                "supplier_plan_id": F.col("s.supplier_plan_id"),
                "suggested_qty": F.col("s.suggested_qty"),
                "standard_batch_size": F.col("s.standard_batch_size"),
                "status": F.col("s.status"),
                "updated_at": F.col("s.updated_at")
            }).whenNotMatchedInsert(values={
                "supplier_plan_id": F.col("s.supplier_plan_id"),
                "shelf_id": F.col("s.shelf_id"),
                "suggested_qty": F.col("s.suggested_qty"),
                "standard_batch_size": F.col("s.standard_batch_size"),
                "status": F.col("s.status"),
                "created_at": F.col("s.created_at"),
                "updated_at": F.col("s.updated_at")
            }).execute())
            return
        except Exception as e:
            last = e
            if not _is_delta_conflict(e) or attempt == DELTA_WRITE_MAX_RETRIES:
                raise
            sleep_s = DELTA_WRITE_RETRY_BASE_S * attempt
            print(f"[delta-retry] wh_alert_engine merge conflict ({attempt}/{DELTA_WRITE_MAX_RETRIES}) -> sleep {sleep_s}s: {e}")
            time.sleep(sleep_s)
    raise last  # pragma: no cover

def _filter_recent_alerts(alerts_df, cutoff_ts):
    if ALERTS_COOLDOWN_MINUTES <= 0:
        return alerts_df
    if not DeltaTable.isDeltaTable(spark, DL_ALERTS_PATH):
        return alerts_df
    try:
        recent = (
            spark.read.format("delta").load(DL_ALERTS_PATH)
            .where(F.col("created_at") >= F.lit(cutoff_ts))
            .select("event_type", "shelf_id", "location")
            .dropDuplicates()
        )
        return alerts_df.join(recent, on=["event_type", "shelf_id", "location"], how="left_anti")
    except Exception as e:
        print(f"[alerts-dedupe] warn: {e}")
        return alerts_df

def kafka_topic_exists(topic_name: str) -> bool:
    """Check if a Kafka topic exists to avoid UnknownTopic errors when reading offsets."""
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
# Schemas
# =========================
schema_wh_state = T.StructType([
    T.StructField("shelf_id", T.StringType()),
    T.StructField("wh_current_stock", T.IntegerType()),
    T.StructField("last_update_ts", T.TimestampType()),
])

schema_policy = T.StructType([
    T.StructField("policy_id", T.StringType()),
    T.StructField("shelf_id", T.StringType()),
    T.StructField("item_category", T.StringType()),
    T.StructField("item_subcategory", T.StringType()),
    T.StructField("reorder_point_qty", T.IntegerType()),
    T.StructField("active", T.BooleanType()),
    T.StructField("notes", T.StringType()),
    T.StructField("updated_at", T.TimestampType()),
])

schema_wh_batch = T.StructType([
    T.StructField("shelf_id", T.StringType()),
    T.StructField("batch_code", T.StringType()),
    T.StructField("received_date", T.DateType()),
    T.StructField("expiry_date", T.DateType()),
    T.StructField("batch_quantity_warehouse", T.IntegerType()),
    T.StructField("batch_quantity_store", T.IntegerType()),
    T.StructField("last_update_ts", T.TimestampType()),
])

schema_wh_supplier_plan = T.StructType([
    T.StructField("supplier_plan_id", T.StringType()),
    T.StructField("shelf_id", T.StringType()),
    T.StructField("suggested_qty", T.IntegerType()),
    T.StructField("standard_batch_size", T.IntegerType()),
    T.StructField("status", T.StringType()),
    T.StructField("created_at", T.TimestampType()),
    T.StructField("updated_at", T.TimestampType()),
])

# =========================
# Static snapshots: policies (latest) and standard_batch_size (optional)
# =========================
def bootstrap_wh_policies_if_missing():
    """
    If the wh_policies topic is empty and BOOTSTRAP_WH_POLICIES_FROM_PG=1,
    read policies from Postgres and publish them to Kafka (compacted).
    Key = shelf_id (if NULL, use '__GLOBAL__' as a global fallback).
    """
    if not BOOTSTRAP_WH_POLICIES_FROM_PG:
        print("[wh-policies-bootstrap] BOOTSTRAP_WH_POLICIES_FROM_PG=0 -> skip.")
        return

    # 1) Does the topic already have data?
    if kafka_topic_exists(TOPIC_WH_POLICIES):
        try:
            existing = (
                spark.read.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKER)
                .option("subscribe", TOPIC_WH_POLICIES)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load()
            )
            if existing.limit(1).count() > 0:
                print(f"[wh-policies-bootstrap] Topic {TOPIC_WH_POLICIES} is not empty -> skip.")
                return
        except Exception as e:
            print(f"[wh-policies-bootstrap] warning while checking topic: {e}")
    else:
        print(f"[wh-policies-bootstrap] Topic {TOPIC_WH_POLICIES} does not exist yet -> bootstrap required.")

    # 2) JDBC parameters
    if not (JDBC_PG_URL and JDBC_PG_USER and JDBC_PG_PASSWORD):
        print("[wh-policies-bootstrap] Missing JDBC params -> skip.")
        return

    # 3) Read Postgres and keep the latest policy per shelf_id (active=true)
    last_err = None
    for attempt in range(1, 6):
        try:
            base = (
                spark.read.format("jdbc")
                .option("url", JDBC_PG_URL)
                .option("user", JDBC_PG_USER)
                .option("password", JDBC_PG_PASSWORD)
                .option(
                    "dbtable",
                    f"(SELECT policy_id, shelf_id, item_category, item_subcategory, "
                    f"reorder_point_qty, active, notes, updated_at "
                    f"FROM {WH_POLICIES_PG_TABLE}) t"
                )
                .load()
            )
            break
        except Exception as e:
            last_err = e
            print(f"[wh-policies-bootstrap] read failed ({attempt}/5): {e}")
            time.sleep(3)
    else:
        raise RuntimeError("Unable to read wh_policies from Postgres") from last_err

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
                    "reorder_point_qty", "active", "notes", "updated_at"
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
        .option("topic", TOPIC_WH_POLICIES) \
        .save()

    print(f"[wh-policies-bootstrap] Published initial wh_policies to topic {TOPIC_WH_POLICIES}.")

def load_policies_latest():
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC_WH_POLICIES)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        df.select(
            F.col("key").cast("string"),
            F.col("value").cast("string").alias("value_str"),
            "partition","offset","timestamp"
        )
        .withColumn("value_json", F.from_json("value_str", schema_policy))
        .select(
            F.col("value_json.shelf_id").alias("shelf_id"),
            F.col("value_json.reorder_point_qty").alias("reorder_point_qty"),
            F.col("value_json.active").alias("active"),
            "partition","offset","timestamp"
        )
        .filter(F.col("active") == True)
    )

    w = Window.partitionBy("shelf_id").orderBy(F.col("offset").desc())
    latest = parsed.withColumn("rn", F.row_number().over(w)).where("rn=1").drop("rn","partition","offset")
    return latest.cache()

bootstrap_wh_policies_if_missing()
POLICIES_LATEST = load_policies_latest()

# Optional: infer a per-shelf standard_batch_size from wh_batch_state mirror in Delta (mode/max)
# If not available, we'll leave it null and not round.

def standard_batch_sizes_from_delta() -> Optional["pyspark.sql.dataframe.DataFrame"]:
    try:
        wh_batches = spark.read.format("delta").load(DL_WH_BATCH_PATH)
        sizes = (
            wh_batches
            .select("shelf_id", "batch_code")
            .withColumn("standard_batch_size", F.lit(None).cast("int"))  # placeholder in case you add it later
            .groupBy("shelf_id")
            .agg(F.max("standard_batch_size").alias("standard_batch_size"))
        )
        return sizes
    except Exception as e:
        print(f"[std_batch_size] warn: {e}")
        return None

STD_BATCH_SIZES_PG_TABLE = os.getenv("STD_BATCH_SIZES_PG_TABLE", "config.batch_catalog")


def standard_batch_sizes_from_pg() -> Optional["pyspark.sql.dataframe.DataFrame"]:
    if not (JDBC_PG_URL and JDBC_PG_USER and JDBC_PG_PASSWORD):
        return None
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", JDBC_PG_URL)
            .option("user", JDBC_PG_USER)
            .option("password", JDBC_PG_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", STD_BATCH_SIZES_PG_TABLE)
            .load()
        )
        if not {"shelf_id", "standard_batch_size"}.issubset(set(df.columns)):
            return None
        return (
            df.select("shelf_id", "standard_batch_size")
            .where(F.col("shelf_id").isNotNull())
            .where(F.col("standard_batch_size").isNotNull() & (F.col("standard_batch_size") > 0))
            .groupBy("shelf_id")
            .agg(F.max("standard_batch_size").alias("standard_batch_size"))
        )
    except Exception as e:
        print(f"[std_batch_size] warn (pg): {e}")
        return None


# Prefer PG snapshot (real batch sizes); fallback to Delta placeholder (may be null-only).
STD_BATCH_SIZES = standard_batch_sizes_from_pg() or standard_batch_sizes_from_delta()

# =========================
# Streaming source: wh_state
# =========================
raw_state = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_WH_STATE)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "true" if FAIL_ON_DATA_LOSS else "false")
    .load()
)

wh_state_stream = (
    raw_state
    .select(F.col("value").cast("string").alias("value_str"))
    .withColumn("json", F.from_json("value_str", schema_wh_state))
    .select("json.*")
    .filter(F.col("shelf_id").isNotNull())
)

# =========================
# ForeachBatch logic
# =========================

def foreach_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    sim_now = get_simulated_now()
    cooldown_ts = sim_now - timedelta(minutes=ALERTS_COOLDOWN_MINUTES)

    shelves = batch_df.select("shelf_id", "wh_current_stock").distinct()

    # Join with per-shelf policy (latest) – no global here; if missing, set a conservative default
    pol = POLICIES_LATEST
    joined = (
        shelves.alias("s").join(pol.alias("p"), on="shelf_id", how="left")
        .withColumn("reorder_point_qty", F.coalesce(F.col("p.reorder_point_qty"), F.lit(10)))
    )

    # Near-expiry: find soonest expiry per shelf in warehouse
    try:
        wh_batches = (
            spark.read.format("delta")
            .load(DL_WH_BATCH_PATH)
            .select("shelf_id", "expiry_date", "batch_quantity_warehouse")
        )
        soonest = (
            wh_batches
            .groupBy("shelf_id")
            .agg(
                F.min("expiry_date").alias("min_expiry"),
                F.sum("batch_quantity_warehouse").alias("qty_on_batches")
            )
        )
    except Exception as e:
        print(f"[near_expiry_wh] warn: {e}")
        soonest = spark.createDataFrame([], schema=T.StructType([
            T.StructField("shelf_id", T.StringType()),
            T.StructField("min_expiry", T.DateType()),
            T.StructField("qty_on_batches", T.IntegerType()),
        ]))

    enriched = (
        joined.join(soonest, on="shelf_id", how="left")
        .withColumn("days_to_expiry", F.datediff(F.col("min_expiry"), F.to_date(F.lit(sim_now))))
    )

    # 1) Reorder alert if below reorder_point
    reorder_needed = (
        enriched.filter(F.col("wh_current_stock") < F.col("reorder_point_qty"))
        .withColumn("event_type", F.lit("supplier_request"))
        .withColumn("location", F.lit("warehouse"))
        .withColumn("severity", F.lit("medium"))
    )

    # 2) Near-expiry alert in warehouse
    near_expiry = (
        enriched.filter(
            F.col("min_expiry").isNotNull() &
            (F.col("days_to_expiry") <= F.lit(NEAR_EXPIRY_DAYS)) &
            (F.col("days_to_expiry") >= 0)
        )
        .withColumn("event_type", F.lit("near_expiry"))
        .withColumn("location", F.lit("warehouse"))
        .withColumn("severity", F.lit("high"))
    )

    # Compose alerts rows (append-only)
    reorder_alerts = (
        reorder_needed.select(
            "event_type","shelf_id","location","severity",
            F.col("wh_current_stock").alias("current_stock"),
            F.col("reorder_point_qty").alias("min_qty"),
            F.lit(None).cast("double").alias("threshold_pct"),
            F.lit(None).cast("double").alias("stock_pct"),
            # suggested for supplier plan calculated below; here 0 just for alert payload
            F.lit(0).alias("suggested_qty")
        )
    )

    near_expiry_alerts = (
        near_expiry.select(
            "event_type","shelf_id","location","severity",
            F.col("qty_on_batches").alias("current_stock"),
            F.lit(None).cast("int").alias("min_qty"),
            F.lit(None).cast("double").alias("threshold_pct"),
            F.lit(None).cast("double").alias("stock_pct"),
            F.lit(0).alias("suggested_qty")
        )
    )

    alerts_df = reorder_alerts.unionByName(near_expiry_alerts, allowMissingColumns=True)\
        .withColumn("created_at", F.lit(sim_now))

    alerts_df = _filter_recent_alerts(alerts_df, cooldown_ts)

    if alerts_df.rdd.isEmpty() is False:
        # Normalize types to match the existing Delta table schema (prevents DELTA_FAILED_TO_MERGE_FIELDS on int/long).
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

        # Kafka append-only
        out = alerts_df.withColumn("value", F.to_json(F.struct(
            "event_type","shelf_id","location","severity",
            "current_stock","min_qty","threshold_pct","stock_pct","suggested_qty","created_at"
        ))).select(F.lit(None).cast("string").alias("key"), "value")

        out.write.format("kafka")\
	       .option("kafka.bootstrap.servers", KAFKA_BROKER)\
	       .option("topic", TOPIC_ALERTS)\
	       .save()

        _write_alerts_delta_with_retries(alerts_to_delta, batch_id)

    if not WH_SUPPLIER_PLAN_ENABLED:
        return

    # Supplier plan (compacted): compute suggested_qty when below reorder point
    plans_base = reorder_needed.select(
        "shelf_id", "wh_current_stock", "reorder_point_qty"
    )

    if STD_BATCH_SIZES is not None:
        plans_base = plans_base.join(STD_BATCH_SIZES, on="shelf_id", how="left")
    else:
        plans_base = plans_base.withColumn("standard_batch_size", F.lit(None).cast("int"))

    plans_raw = (
        plans_base
        .withColumn("raw_suggested", F.greatest(F.lit(0), F.col("reorder_point_qty") - F.col("wh_current_stock")))
        .withColumn(
            "suggested_qty",
            F.when(
                F.col("standard_batch_size").isNotNull() & (F.col("standard_batch_size") > 0),
                (
                    (
                        (F.col("raw_suggested") + (F.col("standard_batch_size") * DEFAULT_MULTIPLIER) - 1)
                        / (F.col("standard_batch_size") * DEFAULT_MULTIPLIER)
                    ).cast("int")
                    * (F.col("standard_batch_size") * DEFAULT_MULTIPLIER)
                )
            ).otherwise(F.col("raw_suggested"))
        )
        .filter(F.col("suggested_qty") > 0)
        .select("shelf_id", "suggested_qty", "standard_batch_size")
    )

    # COLLAPSE: guarantee 1 row per shelf_id
    plans = (
        plans_raw
        .groupBy("shelf_id")
        .agg(
            F.max("suggested_qty").alias("suggested_qty"),
            F.max("standard_batch_size").alias("standard_batch_size")
        )
        .withColumn("supplier_plan_id", F.expr("uuid()"))
        .withColumn("status", F.lit("pending"))
        .withColumn("created_at", F.lit(get_simulated_now()))
        .withColumn("updated_at", F.lit(get_simulated_now()))
    )


    if plans.rdd.isEmpty() is False:
        # Kafka compacted (key = shelf_id)
        kafka_plans = plans.withColumn("value", F.to_json(F.struct(
            "supplier_plan_id","shelf_id","suggested_qty","standard_batch_size","status","created_at","updated_at"
        ))).select(F.col("shelf_id").alias("key"), "value")

        kafka_plans.write.format("kafka")\
            .option("kafka.bootstrap.servers", KAFKA_BROKER)\
            .option("topic", TOPIC_WH_SUPPLIER_PLAN)\
            .save()

        # Delta mirror (upsert latest per shelf)
        if DeltaTable.isDeltaTable(spark, DL_SUPPLIER_PLAN_PATH):
            _merge_supplier_plans_with_retries(plans)
        else:
            plans.write.format("delta").mode("overwrite").save(DL_SUPPLIER_PLAN_PATH)

# =========================
# Start streaming
# =========================
query = (
    wh_state_stream.writeStream
    .foreachBatch(foreach_batch)
    .option("checkpointLocation", CKP_FOREACH)
    .start()
)

query.awaitTermination()
