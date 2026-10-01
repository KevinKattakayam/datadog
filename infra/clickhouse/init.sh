#!/bin/bash
set -e
echo "Initializing ClickHouse observability database and least-privilege users..."
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS observability"

# Create least-privilege users: writer cannot DROP, analyst is read-only
clickhouse-client --query "CREATE USER IF NOT EXISTS processor IDENTIFIED WITH plaintext_password BY 'processor_dev'"
clickhouse-client --query "GRANT SELECT, INSERT ON observability.* TO processor"

clickhouse-client --query "CREATE USER IF NOT EXISTS grafana_ro IDENTIFIED WITH plaintext_password BY 'grafana_dev'"
clickhouse-client --query "GRANT SELECT ON observability.* TO grafana_ro"

echo "Database 'observability' and role-separated users ready."
