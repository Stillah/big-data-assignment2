#!/bin/bash
set -e

# used by find_hadoop_streaming_jar
export HADOOP_HOME=${HADOOP_HOME:-/usr/local/hadoop}

# Arguments
INPUT_PATH="${1:-/data}"
CASSANDRA_HOST="${2:-scylla-master}"
CASSANDRA_PORT="${3:-9042}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Main pipeline
echo ""
echo "Starting Index Pipeline"

echo ""
echo "Step 1: Creating Index from Documents"
echo "Input path: $INPUT_PATH"

if [ ! -f "$SCRIPT_DIR/create_index.sh" ]; then
    echo "Error: create_index.sh not found in $SCRIPT_DIR"
    exit 1
fi

bash "$SCRIPT_DIR/create_index.sh" "$INPUT_PATH" || exit 1

echo "Index created successfully"

echo ""
echo "Step 2: Storing Index in ScyllaDB"
echo "ScyllaDB host: $CASSANDRA_HOST"

if [ ! -f "$SCRIPT_DIR/store_index.sh" ]; then
    echo "Error: store_index.sh not found in $SCRIPT_DIR"
    exit 1
fi

bash "$SCRIPT_DIR/store_index.sh" \
    --cassandra-host "$CASSANDRA_HOST" \
    --create-keyspace || error "Index storage failed"
status "Index stored successfully"

# Complete
echo ""
echo "Indexing Complete!"
echo "Index is now available in ScyllaDB for search queries"
echo ""