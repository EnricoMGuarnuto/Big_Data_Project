FROM python:3.11-slim

# Installa netcat-openbsd e curl
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    netcat-openbsd curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Scarica mc (MinIO client) con retry
RUN curl --fail --retry 5 -O https://dl.min.io/client/mc/release/linux-amd64/mc && \
    chmod +x mc && mv mc /usr/local/bin/

COPY minio_init.sh /init.sh
CMD ["/bin/sh", "/init.sh"]
