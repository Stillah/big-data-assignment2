#!/bin/bash
set -e

# Color codes
COLOR_RESET='\033[0m'
COLOR_GREEN='\033[0;32m'
COLOR_YELLOW='\033[1;33m'
COLOR_RED='\033[0;31m'
COLOR_BLUE='\033[0;34m'

# Configuration
LOCAL_FILE="$1"
CASSANDRA_HOST="${2:-localhost}"
CASSANDRA_PORT="${3:-7000}"
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
header() { echo -e "\n${COLOR_BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${COLOR_RESET}\n${COLOR_BLUE}$1${COLOR_RESET}\n${COLOR_BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${COLOR_RESET}"; }
status() { echo -e "${COLOR_GREEN}✓${COLOR_RESET} $1"; }
error() { echo -e "${COLOR_RED}✗${COLOR_RESET} $1"; exit 1; }
warn() { echo -e "${COLOR_YELLOW}⚠${COLOR_RESET} $1"; }
info() { echo "  $1"; }


# Find Hadoop streaming JAR
find_hadoop_streaming_jar() {
    # Use HADOOP_HOME as primary source (set by start-services.sh)
    if [ -z "$HADOOP_HOME" ]; then
        HADOOP_HOME=$(dirname "$(dirname "$(which hadoop)")")
    fi
    
    # The JAR is standard in HADOOP_HOME/share/hadoop/tools/lib/ with version number
    # Example: hadoop-streaming-3.3.1.jar
    if [ -d "$HADOOP_HOME/share/hadoop/tools/lib" ]; then
        local jar=$(ls "$HADOOP_HOME/share/hadoop/tools/lib"/hadoop-streaming-*.jar 2>/dev/null | head -1)
        if [ -f "$jar" ]; then
            echo "$jar"
            return 0
        fi
    fi
    
    # Fallback to non-versioned name
    local fallback_paths=(
        "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming.jar"
        "$HADOOP_HOME/share/hadoop/mapreduce/hadoop-streaming.jar"
    )
    
    for path in "${fallback_paths[@]}"; do
        if [ -f "$path" ]; then
            echo "$path"
            return 0
        fi
    done
    
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
info "Input file: $LOCAL_FILE"

if [ -z "$LOCAL_FILE" ]; then
    error "No file specified. Usage: ./add_to_index.sh <LOCAL_FILE_PATH>"
fi

if [ ! -f "$LOCAL_FILE" ]; then
    error "File not found: $LOCAL_FILE"
fi

status "Input file exists"

# Check file is not empty
if [ ! -s "$LOCAL_FILE" ]; then
    error "File is empty: $LOCAL_FILE"
fi

status "Input file is not empty"

# Validate MapReduce scripts
header "Validating MapReduce Scripts"

for script in "$MAPPER1" "$REDUCER1" "$MAPPER2" "$REDUCER2"; do
    if [ ! -f "$script" ]; then
        error "Required script not found: $script"
    fi
done
status "All MapReduce scripts found"

# Validate Hadoop
header "Validating Hadoop Environment"

if ! command -v hadoop &>/dev/null; then
    error "Hadoop not found in PATH"
fi
status "Hadoop found"

if ! hadoop fs -ls / &>/dev/null; then
    error "Cannot access HDFS"
fi
status "HDFS is accessible"

# Find Hadoop streaming JAR
if ! STREAMING_JAR=$(find_hadoop_streaming_jar); then
    error "Hadoop streaming JAR not found"
fi
status "Found streaming JAR: $STREAMING_JAR"

# Create temporary directories
header "Setting Up Temporary Directories"

mkdir -p "$LOCAL_TEMP_DIR"
status "Local temp directory created: $LOCAL_TEMP_DIR"

# Clean and create HDFS temp directories
for dir in "$HDFS_TEMP_INPUT" "$HDFS_TEMP_STAGE1" "$HDFS_TEMP_STAGE2"; do
    if hadoop fs -test -d "$dir" 2>/dev/null; then
        hadoop fs -rm -r "$dir" 2>/dev/null || true
    fi
    hadoop fs -mkdir -p "$dir"
done
status "HDFS temp directories created"

# Upload file to HDFS
header "Uploading Document to HDFS"

doc_basename=$(basename "$LOCAL_FILE")
hdfs_file="$HDFS_TEMP_INPUT/$doc_basename"

hadoop fs -put "$LOCAL_FILE" "$hdfs_file"
status "Document uploaded to HDFS: $hdfs_file"

# Run Pipeline 1: Tokenization and Indexing
header "Running Pipeline 1: Document Tokenization and Indexing"

start_time=$(date +%s)

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.task.timeout=1200000 \
    -D mapreduce.reduce.shuffle.connect.timeout=180000 \
    -D mapreduce.reduce.shuffle.read.timeout=180000 \
    -D dfs.socket.timeout=180000 \
    -input "$HDFS_TEMP_INPUT" \
    -output "$HDFS_TEMP_STAGE1" \
    -mapper "python3 $MAPPER1" \
    -reducer "python3 $REDUCER1" \
    -file "$MAPPER1" \
    -file "$REDUCER1" || error "Pipeline 1 failed"

end_time=$(date +%s)
pipeline1_time=$((end_time - start_time))

status "Pipeline 1 completed in ${pipeline1_time}s"

# Run Pipeline 2: Statistics and Index Finalization
header "Running Pipeline 2: Statistics and Index Finalization"

start_time=$(date +%s)

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.task.timeout=1200000 \
    -D mapreduce.reduce.shuffle.connect.timeout=180000 \
    -D mapreduce.reduce.shuffle.read.timeout=180000 \
    -D dfs.socket.timeout=180000 \
    -input "$HDFS_TEMP_STAGE1" \
    -output "$HDFS_TEMP_STAGE2" \
    -mapper "python3 $MAPPER2" \
    -reducer "python3 $REDUCER2" \
    -file "$MAPPER2" \
    -file "$REDUCER2" || error "Pipeline 2 failed"

end_time=$(date +%s)
pipeline2_time=$((end_time - start_time))

status "Pipeline 2 completed in ${pipeline2_time}s"

# Download index from HDFS
header "Downloading Index Data"

hadoop fs -getmerge "$HDFS_TEMP_STAGE2" "$LOCAL_INDEX_FILE"
status "Index downloaded to local: $LOCAL_INDEX_FILE"

# Verify index file
if [ ! -s "$LOCAL_INDEX_FILE" ]; then
    error "Index file is empty"
fi

status "Index file is valid"

# Update ScyllaDB
header "Updating ScyllaDB Index Tables"

python3 "$SCRIPT_DIR/update_index_in_cassandra.py" \
    --index-path "$LOCAL_INDEX_FILE" \
    --cassandra-host "$CASSANDRA_HOST" \
    --cassandra-port "$CASSANDRA_PORT" \
    --keyspace "$KEYSPACE_NAME" || error "ScyllaDB update failed"

status "ScyllaDB tables updated successfully"

# Summary
header "Document Added to Index Successfully!"

echo "Summary:"
echo "  Document:    $(basename "$LOCAL_FILE")"
echo "  File size:   $(wc -c < "$LOCAL_FILE") bytes"
echo "  ScyllaDB:    $CASSANDRA_HOST:$CASSANDRA_PORT"
echo "  Keyspace:    $KEYSPACE_NAME"
echo ""
echo "The document has been indexed and added to the searchable index."
echo ""
