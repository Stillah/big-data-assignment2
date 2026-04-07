#!/bin/bash
# Create a tiny test file locally and upload to HDFS
hadoop fs -get "/data/73284906_A_Guilty_Conscience_(2023_film).txt" /tmp/test.txt
hadoop fs -mkdir -p /input/data
hadoop fs -put -f /tmp/test.txt /input/data/

# Run with single mapper, no memory limits
hadoop fs -rm -r /indexer/test_output
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -input /input/data \
  -output /indexer/test_output \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -file mapreduce/mapper1.py -file mapreduce/reducer1.py