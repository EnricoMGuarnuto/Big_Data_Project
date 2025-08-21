#!/bin/sh

echo "[minio-init] Waiting for MinIO to be reachable..."

# Aspetta MinIO
for i in $(seq 1 30); do
  nc -z minio 9000 && break
  echo "⏳ Waiting for minio:9000... ($i)"
  sleep 1
done

if ! nc -z minio 9000; then
  echo "❌ MinIO non è disponibile su minio:9000"
  exit 1
fi

# Alias MinIO client (mc)
mc alias set local http://minio:9000 minio minio123

# Crea bucket principale se non esiste
if ! mc ls local/retail-lake > /dev/null 2>&1; then
  echo "[minio-init] ✅ Creating bucket 'retail-lake'..."
  mc mb local/retail-lake
else
  echo "[minio-init] ✅ Bucket 'retail-lake' already exists."
fi

# Crea cartelle logiche (non obbligatorie ma utili)
mc mb --ignore-existing local/retail-lake/bronze
mc mb --ignore-existing local/retail-lake/silver
mc mb --ignore-existing local/retail-lake/checkpoints

echo "[minio-init] ✅ Structure ready: retail-lake/{bronze,silver,checkpoints}"
