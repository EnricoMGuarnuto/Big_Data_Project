#!/bin/sh

echo "[minio-init] Waiting for MinIO to be reachable..."

# Aspetta che MinIO risponda sulla porta 9000 (fino a 30 secondi)
for i in $(seq 1 30); do
  nc -z minio 9000 && break
  echo "⏳ Waiting for minio:9000... ($i)"
  sleep 1
done

# Verifica ancora una volta
if ! nc -z minio 9000; then
  echo "❌ MinIO non è disponibile su minio:9000"
  exit 1
fi

# Imposta alias corretto (niente localhost!)
mc alias set local http://minio:9000 minio minio123

# Crea bucket solo se non esiste
if ! mc ls local/retail-lake > /dev/null 2>&1; then
  echo "[minio-init] ✅ Creating bucket 'retail-lake'..."
  mc mb local/retail-lake
else
  echo "[minio-init] ✅ Bucket 'retail-lake' already exists."
fi
