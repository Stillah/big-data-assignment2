#!/bin/bash

set -e

# Colors for output
CASSANDRA_HOST="${CASSANDRA_HOST:-scylla-master}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"

QUERY="$1"
status "Query: $QUERY"

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Activate virtual environment if it exists
if [ -f "$SCRIPT_DIR/.venv/bin/activate" ]; then
    echo "Activating Virtual Environment"
    source "$SCRIPT_DIR/.venv/bin/activate"
fi

# Configure PySpark environment
echo "Configuring PySpark"

export PYSPARK_DRIVER_PYTHON=$(which python3)
export PYSPARK_PYTHON=./.venv/bin/python

echo "Submitting Search Query"
echo "Query: $QUERY"
echo "Master: yarn"
echo ""

spark-submit \
    --master yarn \
    --deploy-mode client \
    --packages com.scylladb:spark-scylladb-connector_2.12:4.0.0,com.github.jnr:jnr-posix:3.1.16 \
    --archives "$SCRIPT_DIR/.venv.tar.gz#.venv" \
    --conf spark.executorEnv.PYSPARK_PYTHON="./.venv/bin/python" \
    --conf spark.executorEnv.PYSPARK_DRIVER_PYTHON="$PYSPARK_DRIVER_PYTHON" \
    --conf spark.network.timeout=180s \
    --conf spark.rpc.askTimeout=180s \
    --conf spark.cassandra.connection.host="$CASSANDRA_HOST" \
    --conf spark.cassandra.connection.port="$CASSANDRA_PORT" \
    --conf spark.cassandra.connection.timeoutMS=60000 \
    "$SCRIPT_DIR/query.py" \
    --query "$QUERY" \
    --cassandra-host "$CASSANDRA_HOST" \
    --cassandra-port "$CASSANDRA_PORT" || exit 1

echo ""
echo "Search completed"
echo ""