#!/bin/bash

# Wait for Trino to be ready
echo "Waiting for Trino to be ready..."
until curl -s http://trino:8080/v1/info | grep -q '"starting":false'; do
  sleep 5
done

echo "Trino is ready. Running initialization..."

# Create schema
trino --server http://trino:8080 --execute "CREATE SCHEMA IF NOT EXISTS delta.default"

# Register table
trino --server http://trino:8080 --execute "CALL delta.system.register_table(schema_name => 'default', table_name => 'logistics_data', table_location => 's3a://delta/logistics_data/')"

echo "Initialization complete."
