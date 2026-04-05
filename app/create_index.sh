#!/bin/bash

set -e  # Exit on error

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

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

# Hadoop streaming JAR location (auto-detect)
find_streaming_jar() {
    local jar=$(ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar 2>/dev/null | head -1)
    [ -n "$jar" ] && echo "$jar" || echo "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming.jar"
}

STREAMING_JAR=$(find_streaming_jar)
if [ ! -f "$STREAMING_JAR" ]; then
    echo -e "${RED}Error: Hadoop streaming JAR not found${NC}"
    exit 1
fi

chmod +x "$MAPPER1" "$REDUCER1" "$MAPPER2" "$REDUCER2"

# Clean previous outputs
hadoop fs -rm -r "$HDFS_TEMP" "$HDFS_OUTPUT" 2>/dev/null || true
echo -e "${GREEN}Cleaned old outputs${NC}"

# ==================== PIPELINE 1 ====================
echo -e "\n${YELLOW}Running Pipeline 1: Tokenization & Term Extraction${NC}"
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -D dfs.client.socket-timeout=60000 \
    -D dfs.client.use.datanode.hostname=true\
    -input "/input/data" \
    -output "$HDFS_TEMP" \
    -mapper "python3 -u mapper1.py" \
    -reducer "python3 -u reducer1.py" \
    -file "$MAPPER1" \
    -file "$REDUCER1"

echo -e "${GREEN}Pipeline 1 completed${NC}"

# ==================== PIPELINE 2 ====================
echo -e "\n${YELLOW}Running Pipeline 2: Index Finalization${NC}"
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.reduces=1 \
    -D dfs.client.socket-timeout=60000 \
    -D dfs.client.use.datanode.hostname=true\
    -input "/input/data" \
    -output "$HDFS_OUTPUT" \
    -mapper "python3 -u mapper2.py" \
    -reducer "python3 -u reducer2.py" \
    -file "$MAPPER2" \
    -file "$REDUCER2"

echo -e "${GREEN} Pipeline 2 completed${NC}"

# Summary
echo -e "\n${GREEN}Index creation finished successfully!${NC}"
echo "Final index location: $HDFS_OUTPUT"
echo "Sample entries:"
hadoop fs -cat "$HDFS_OUTPUT/part-r-*" 2>/dev/null | head -5 | while read line; do
    echo "  $line"
done

echo -e "\n${GREEN}All done.${NC}"