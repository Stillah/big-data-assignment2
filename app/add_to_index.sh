#!/bin/bash
set -e

# Configuration
LOCAL_FILE="$1"
CASSANDRA_HOST="${2:-scylla-master}"
CASSANDRA_PORT="${3:-9042}"
KEYSPACE_NAME="${4:-search_index}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAPREDUCE_DIR="${SCRIPT_DIR}/mapreduce"
MAPPER1="${MAPREDUCE_DIR}/mapper1.py"
REDUCER1="${MAPREDUCE_DIR}/reducer1.py"
MAPPER2="${MAPREDUCE_DIR}/mapper2.py"
REDUCER2="${MAPREDUCE_DIR}/reducer2.py"

# Temporary paths
HDFS_TEMP_INPUT="/tmp/add_to_index/input"
HDFS_TEMP_STAGE1="/tmp/add_to_index/stage1"
HDFS_TEMP_STAGE2="/tmp/add_to_index/stage2"
LOCAL_TEMP_DIR="/tmp/add_to_index_$$"
LOCAL_INDEX_FILE="$LOCAL_TEMP_DIR/index.txt"

# Helper functions
header() { echo ""; echo "--- $1 ---"; }
status() { echo "Done: $1"; }
error() { echo "Error: $1"; exit 1; }
warn() { echo "Warning: $1"; }
info() { echo "  $1"; }


# Find Hadoop streaming JAR
find_hadoop_streaming_jar() {
    # Use HADOOP_HOME as primary source (which is set by start-services.sh)
    if [ -z "$HADOOP_HOME" ]; then
        HADOOP_HOME=$(dirname "$(dirname "$(which hadoop)")")
    fi
    
    # Check jar file
    if [ -d "$HADOOP_HOME/share/hadoop/tools/lib" ]; then
        local jar=$(ls "$HADOOP_HOME/share/hadoop/tools/lib"/hadoop-streaming-*.jar 2>/dev/null | head -1)
        if [ -f "$jar" ]; then
            echo "$jar"
            return 0
        fi
    fi
    
    return 1
}

# Cleanup on exit
cleanup() {
    if [ -n "$1" ]; then
        error "$1"
    fi
    
    warn "Cleaning up temporary files"
    
    # Remove HDFS temp files
    if hadoop fs -test -d "$HDFS_TEMP_INPUT" 2>/dev/null; then
        hadoop fs -rm -r "$HDFS_TEMP_INPUT" 2>/dev/null || true
    fi
    if hadoop fs -test -d "$HDFS_TEMP_STAGE1" 2>/dev/null; then
        hadoop fs -rm -r "$HDFS_TEMP_STAGE1" 2>/dev/null || true
    fi
    if hadoop fs -test -d "$HDFS_TEMP_STAGE2" 2>/dev/null; then
        hadoop fs -rm -r "$HDFS_TEMP_STAGE2" 2>/dev/null || true
    fi
    
    # Remove local temp files
    rm -rf "$LOCAL_TEMP_DIR" 2>/dev/null || true
    
    status "Temporary files cleaned up"
}

trap 'cleanup "$@"' EXIT

# Main pipeline
header "Adding Document to Index"

# Validate input file
if [ -z "$LOCAL_FILE" ]; then
    error "No file specified. Usage: ./add_to_index.sh <LOCAL_FILE_PATH>"
fi

if [ ! -f "$LOCAL_FILE" ]; then
    error "File not found: $LOCAL_FILE"
fi

if [ ! -s "$LOCAL_FILE" ]; then
    error "File is empty: $LOCAL_FILE"
fi

# Validate MapReduce scripts exist
for script in "$MAPPER1" "$REDUCER1" "$MAPPER2" "$REDUCER2"; do
    if [ ! -f "$script" ]; then
        error "Required script not found: $script"
    fi
done

# Validate Hadoop works
if ! command -v hadoop &>/dev/null; then
    error "Hadoop not found in PATH"
fi

if ! hadoop fs -ls / &>/dev/null; then
    error "Cannot access HDFS"
fi

# Find and validate Hadoop streaming JAR
if ! STREAMING_JAR=$(find_hadoop_streaming_jar); then
    error "Hadoop streaming JAR not found"
fi

# Setup and upload
mkdir -p "$LOCAL_TEMP_DIR"
# hadoop fs -mkdir -p "$HDFS_TEMP_INPUT" "$HDFS_TEMP_STAGE1" "$HDFS_TEMP_STAGE2"

doc_basename=$(basename "$LOCAL_FILE")
hdfs_file="$HDFS_TEMP_INPUT/$doc_basename"

echo "Uploading document to HDFS: $hdfs_file"
hadoop fs -put "$LOCAL_FILE" "$hdfs_file"
status "Document uploaded to HDFS: $hdfs_file"

# Run Pipeline 1: Tokenization and Indexing
header "Running Pipeline 1"

# Clean output directories before running
echo "Cleaning output directories"
hadoop fs -rm -r "$HDFS_TEMP_STAGE1" 2>/dev/null || true
hadoop fs -rm -r "$HDFS_TEMP_STAGE2" 2>/dev/null || true

start_time=$(date +%s)

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.task.timeout=1200000 \
    -D mapreduce.reduce.shuffle.connect.timeout=180000 \
    -D mapreduce.reduce.shuffle.read.timeout=180000 \
    -D dfs.client.socket-timeout=180000 \
    -input "$HDFS_TEMP_INPUT" \
    -output "$HDFS_TEMP_STAGE1" \
    -mapper "python3 -u mapper1.py" \
    -reducer "python3 -u reducer1.py" \
    -file "$MAPPER1" \
    -file "$REDUCER1" || error "Pipeline 1 failed"

end_time=$(date +%s)
pipeline1_time=$((end_time - start_time))
echo "Pipeline 1 completed in ${pipeline1_time}s"

# Run Pipeline 2: Statistics and Index Finalization
header "Running Pipeline 2"

start_time=$(date +%s)

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.task.timeout=1200000 \
    -D mapreduce.reduce.shuffle.connect.timeout=180000 \
    -D mapreduce.reduce.shuffle.read.timeout=180000 \
    -D dfs.client.socket-timeout=180000 \
    -input "$HDFS_TEMP_STAGE1" \
    -output "$HDFS_TEMP_STAGE2" \
    -mapper "python3 -u mapper2.py" \
    -reducer "python3 -u reducer2.py" \
    -file "$MAPPER2" \
    -file "$REDUCER2" || error "Pipeline 2 failed"

end_time=$(date +%s)
pipeline2_time=$((end_time - start_time))
echo "Pipeline 2 completed in ${pipeline2_time}s"

# Download index from HDFS
header "Downloading Index Data"

hadoop fs -getmerge "$HDFS_TEMP_STAGE2" "$LOCAL_INDEX_FILE"
status "Index downloaded to local: $LOCAL_INDEX_FILE"

if [ ! -s "$LOCAL_INDEX_FILE" ]; then
    error "Index file is empty"
fi

echo "Index downloaded successfully"

# Update ScyllaDB
header "Updating ScyllaDB"

python3 "$SCRIPT_DIR/update_index_in_cassandra.py" \
    --index-path "$LOCAL_INDEX_FILE" \
    --cassandra-host "$CASSANDRA_HOST" \
    --cassandra-port "$CASSANDRA_PORT" \
    --keyspace "$KEYSPACE_NAME" || error "ScyllaDB update failed"

# Summary
header "Document Added to Index Successfully!"

echo ""
echo "Document: $(basename "$LOCAL_FILE")"
echo "File size: $(wc -c < "$LOCAL_FILE") bytes"
echo "ScyllaDB: $CASSANDRA_HOST:$CASSANDRA_PORT"
echo "Keyspace: $KEYSPACE_NAME"
echo ""
