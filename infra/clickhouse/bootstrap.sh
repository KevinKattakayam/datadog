#!/bin/bash
set -euo pipefail

ch() {
  clickhouse-client --host clickhouse "$@"
}

echo "Applying ClickHouse database, roles, and additive local-volume migrations..."
ch --multiquery --query "
  CREATE DATABASE IF NOT EXISTS observability;
  CREATE USER IF NOT EXISTS processor IDENTIFIED WITH plaintext_password BY 'processor_dev';
  GRANT SELECT, INSERT ON observability.* TO processor;
  CREATE USER IF NOT EXISTS grafana_ro IDENTIFIED WITH plaintext_password BY 'grafana_dev';
  GRANT SELECT ON observability.* TO grafana_ro;
  ALTER TABLE observability.metrics ADD COLUMN IF NOT EXISTS tenant_id LowCardinality(String) DEFAULT 'default' AFTER ts;
  ALTER TABLE observability.metrics ADD COLUMN IF NOT EXISTS ingested_at DateTime64(3) DEFAULT now64(3);
"
ch --multiquery < /bootstrap/schema.sql
echo "ClickHouse bootstrap complete."
