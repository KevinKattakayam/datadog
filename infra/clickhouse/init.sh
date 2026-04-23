#!/bin/bash
set -e
echo "Initializing ClickHouse observability database..."
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS observability"
echo "Database 'observability' ready."
