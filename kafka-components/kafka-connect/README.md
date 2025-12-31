# Kafka Connect

This folder contains the Kafka Connect setup used by the stack. The base image
installs the JDBC connector and bundles the Postgres driver so the service can
run without extra mounts.

## Contents
- `Dockerfile`: builds the Kafka Connect image with JDBC + Postgres driver.
- `connect-init/`: bootstrap container that registers connectors via the
  Kafka Connect REST API.

## connect-init (bootstrapper)
`connect-init` is a small Python container that waits for Kafka Connect and
then upserts a set of JDBC source/sink connectors.

It is configurable via environment variables (see `connect-init/init.py`):
- `CONNECT_URL`: Kafka Connect REST endpoint (default `http://kafka-connect:8083`).
- `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASS`: Postgres connection info.
- `CONNECT_INIT_ENABLE_SOURCES`: enable Postgres -> Kafka sources (default `1`).
- `CONNECT_INIT_ENABLE_SINKS`: enable Kafka -> Postgres sinks (default `1`).
- `CONNECT_INIT_ENABLE_ALERTS_SINK`: optional alerts sink (default `0`, Spark
  alerts-sink already writes to Postgres).
- `CONNECT_INIT_ENABLE_POS_SINK`: optional POS sink (default `0`, nested payloads
  don’t map cleanly to JDBC tables).

The full stack runs this service from the root `docker-compose.yml`.
