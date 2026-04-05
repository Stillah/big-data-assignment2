#!/bin/bash
set -e

# used by find_hadoop_streaming_jar
export HADOOP_HOME=${HADOOP_HOME:-/usr/local/hadoop}

# Color codes
COLOR_RESET='\033[0m'
COLOR_BLUE='\033[0;34m'
COLOR_GREEN='\033[0;32m'
COLOR_RED='\033[0;31m'

# Arguments
INPUT_PATH="${1:-/data}"
CASSANDRA_HOST="${2:-scylla-master}"
CASSANDRA_PORT="${3:-9042}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Helper functions
header() { echo -e "\n${COLOR_BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${COLOR_RESET}\n${COLOR_BLUE}$1${COLOR_RESET}\n${COLOR_BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${COLOR_RESET}"; }
status() { echo -e "${COLOR_GREEN}✓${COLOR_RESET} $1"; }
error() { echo -e "${COLOR_RED}✗${COLOR_RESET} $1"; exit 1; }

# Main pipeline
header "Starting Index Pipeline: Create + Store"

header "Step 1: Creating Index from Documents"
echo "Input path: $INPUT_PATH"

if [ ! -f "$SCRIPT_DIR/create_index.sh" ]; then
    error "create_index.sh not found in $SCRIPT_DIR"
fi

bash "$SCRIPT_DIR/create_index.sh" "$INPUT_PATH" || error "Index creation failed"
status "Index created successfully"

header "Step 2: Storing Index in ScyllaDB"
echo "ScyllaDB host: $CASSANDRA_HOST"

if [ ! -f "$SCRIPT_DIR/store_index.sh" ]; then
    error "store_index.sh not found in $SCRIPT_DIR"
fi

bash "$SCRIPT_DIR/store_index.sh" \
    --cassandra-host "$CASSANDRA_HOST" \
    --create-keyspace || error "Index storage failed"
status "Index stored successfully"

# Complete
header "Indexing Complete!"
echo "Index is now available in ScyllaDB for search queries"
echo ""
