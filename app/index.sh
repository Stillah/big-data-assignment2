#!/bin/bash

################################################################################
# index.sh
#
# Complete indexing pipeline:
#   1. Create index from documents using MapReduce (create_index.sh)
#   2. Store index in ScyllaDB (store_index.sh)
#
# Usage:
#   ./index.sh [INPUT_PATH] [CASSANDRA_HOST] [CASSANDRA_PORT]
#
# Arguments:
#   INPUT_PATH        HDFS path to documents (default: /data)
#   CASSANDRA_HOST    ScyllaDB host (default: localhost)
#   CASSANDRA_PORT    ScyllaDB port (default: 9042)
#
# Examples:
#   ./index.sh
#   ./index.sh /data localhost
#   ./index.sh /data scylla.example.com 9042
#
################################################################################

set -e

# Ensure HADOOP_HOME is exported (used by find_hadoop_streaming_jar)
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

# Step 1: Create index using MapReduce
header "Step 1: Creating Index from Documents"
echo "Input path: $INPUT_PATH"

if [ ! -f "$SCRIPT_DIR/create_index.sh" ]; then
    error "create_index.sh not found in $SCRIPT_DIR"
fi

bash "$SCRIPT_DIR/create_index.sh" "$INPUT_PATH" || error "Index creation failed"
status "Index created successfully"

# Step 2: Store index in ScyllaDB
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
