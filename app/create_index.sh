#!/bin/bash

set -e

# Default paths
HDFS_INPUT_PATH="${1:-/input/data}"
HDFS_TEMP="/indexer/pipeline1"
HDFS_OUTPUT="/indexer/final_index"

# Get script directory and set script paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAPREDUCE_DIR="${SCRIPT_DIR}/mapreduce"
MAPPER1="${MAPREDUCE_DIR}/mapper1.py"
REDUCER1="${MAPREDUCE_DIR}/reducer1.py"
MAPPER2="${MAPREDUCE_DIR}/mapper2.py"
REDUCER2="${MAPREDUCE_DIR}/reducer2.py"

# Hadoop streaming JAR location
find_streaming_jar() {
    local jar=$(ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar 2>/dev/null | head -1)
    [ -n "$jar" ] && echo "$jar" || echo "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming.jar"
}

STREAMING_JAR=$(find_streaming_jar)
if [ ! -f "$STREAMING_JAR" ]; then
    echo "Error: Hadoop streaming JAR not found"
    exit 1
fi

chmod +x "$MAPPER1" "$REDUCER1" "$MAPPER2" "$REDUCER2"

# Clean previous outputs
hadoop fs -rm -r "$HDFS_TEMP" "$HDFS_OUTPUT" 2>/dev/null || true
echo "Cleaned old outputs"

echo ""
echo "Running Pipeline 1: Tokenization & Term Extraction\n"
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.maps=4 \
    -D mapreduce.job.reduces=4 \
    -input "$HDFS_INPUT_PATH" \
    -output "$HDFS_TEMP" \
    -mapper "python3 -u mapper1.py" \
    -reducer "python3 -u reducer1.py" \
    -file "$MAPPER1" \
    -file "$REDUCER1"

echo "Pipeline 1 completed"

echo ""
echo "Running Pipeline 2: Index Finalization"
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.maps=4 \
    -D mapreduce.job.reduces=4 \
    -input "$HDFS_TEMP" \
    -output "$HDFS_OUTPUT" \
    -mapper "python3 -u mapper2.py" \
    -reducer "python3 -u reducer2.py" \
    -file "$MAPPER2" \
    -file "$REDUCER2"

echo "Pipeline 2 completed"

echo ""
echo "Index creation finished successfully!"
echo "Final index location: $HDFS_OUTPUT"
echo "Sample entries:"
hadoop fs -cat "$HDFS_OUTPUT/part-*" 2>/dev/null | head -5 | while read line; do
    echo "  $line"
done

echo ""