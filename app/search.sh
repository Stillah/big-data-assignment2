#!/bin/bash

set -e

# Colors for output
COLOR_RESET='\033[0m'
COLOR_GREEN='\033[0;32m'
COLOR_YELLOW='\033[1;33m'
COLOR_RED='\033[0;31m'
COLOR_BLUE='\033[0;34m'

CASSANDRA_HOST="${CASSANDRA_HOST:-scylla-master}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"

# Helper functions
header() { echo -e "\n${COLOR_BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${COLOR_RESET}\n${COLOR_BLUE}$1${COLOR_RESET}\n${COLOR_BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${COLOR_RESET}"; }
status() { echo -e "${COLOR_GREEN}✓${COLOR_RESET} $1"; }
error() { echo -e "${COLOR_RED}✗${COLOR_RESET} $1"; exit 1; }
info() { echo "  $1"; }

QUERY="$1"
status "Query: $QUERY"

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Activate virtual environment if it exists
if [ -f "$SCRIPT_DIR/.venv/bin/activate" ]; then
    header "Activating Virtual Environment"
    source "$SCRIPT_DIR/.venv/bin/activate"
    status "Virtual environment activated"
fi

# Configure PySpark environment
header "Configuring PySpark Environment"

export PYSPARK_DRIVER_PYTHON=$(which python3)
export PYSPARK_PYTHON=./.venv/bin/python

info "Driver Python: $PYSPARK_DRIVER_PYTHON"
info "Executor Python: $PYSPARK_PYTHON"

# Run PySpark job on YARN
header "Submitting Search Query to YARN Cluster"

info "Query: $QUERY"
info "Master: yarn"
info ""

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

status "Search completed"
echo ""