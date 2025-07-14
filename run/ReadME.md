# 🛒 Smart Retail Shelf Restocking — Data Pipeline (Stage 1)

This README documents the **current progress** of the Smart Retail Shelf Restocking project. It includes:

- ✅ Kafka Producers
- ✅ Spark Streaming Consumers to MinIO (Raw Layer)
- ✅ Spark Cleansing Jobs (Cleansed Layer)
- ✅ MinIO structure and instructions to reproduce

---

## 📂 Directory Structure (MinIO - S3A)

```
retail/
├── raw/
│   ├── foot_traffic/
│   ├── pos_transactions/
├── cleansed/
│   ├── foot_traffic/
│   ├── pos_transactions/
```

---

## ⚙️ Technologies Used

- **Apache Kafka**: real-time data streaming
- **Apache Spark**: streaming and batch processing
- **MinIO**: object storage as S3-compatible data lake
- **Parquet** & **JSON**: data formats
- **Docker Compose**: orchestration

---

## ✅ Pipeline Overview

### 1. POS & Traffic Kafka Producers

These Python scripts read Parquet files and push records to Kafka every 0.5s:

```bash
python pos_to_kafka.py --file data/pos_transactions.parquet
python traffic_to_kafka.py --file data/foot_traffic.parquet
```

### 2. Spark Kafka Stream to MinIO (Raw Layer)

These Spark streaming apps consume from Kafka and write JSON to MinIO.

```bash
docker exec -it spark-submit bash
spark-submit --master spark://spark-master:7077 /opt/spark-apps/kafka_pos_to_minio_raw.py
spark-submit --master spark://spark-master:7077 /opt/spark-apps/kafka_traffic_to_minio_raw.py
```

Results saved as:

- `s3a://retail/raw/pos_transactions/dt=YYYY-MM-DD/`
- `s3a://retail/raw/foot_traffic/ingestion_time=.../`

---

## 🧼 Cleansing Stage (Batch Jobs)

### 3. Clean Raw → Cleansed (foot traffic)

```bash
spark-submit --master spark://spark-master:7077 /opt/spark-apps/traffic_raw_to_cleansed.py
```

Saves to:
```
s3a://retail/cleansed/foot_traffic/
```

### 4. Clean Raw → Cleansed (POS transactions)

```bash
spark-submit --master spark://spark-master:7077 /opt/spark-apps/pos_raw_to_cleansed.py
```

Saves to:
```
s3a://retail/cleansed/pos_transactions/
```

---

## ✅ Output Samples

- `part-00000-*.json`: raw Kafka data saved as JSON
- `part-00000-*.snappy.parquet`: cleansed data in Parquet format

---

## 📌 Next Steps

1. Simulate shelf sensor events:  
   ```bash
    spark-submit --master spark://spark-master:7077 /opt/spark-apps/generate_shelf_sensor_batch.py
   ```

2. Clean shelf events:  
   ```bash
   spark-submit shelf_raw_to_cleansed.py
   ```

3. Join datasets:  
   ```bash
   spark-submit join_curated_pos_shelf_traffic.py
   ```

---
