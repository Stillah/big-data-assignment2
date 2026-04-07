#!/bin/bash

# echo "STARTING NEW prepare_data.sh"
# source .venv/bin/activate
# pip install -r requirements.txt  

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 
export PYSPARK_PYTHON=./.venv/bin/python
MASTER_IP=$(hostname -i | awk '{print $1}')

unset PYSPARK_PYTHON

# DOWNLOAD a.parquet or any parquet file before you run this

hdfs dfs -put -f a.parquet / && \
    spark-submit \
    --archives .venv.tar.gz#.venv \
    --conf spark.driver.host=$MASTER_IP \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.driver.memory=1g \
    --conf spark.executor.memory=1g \
    --conf spark.memory.offHeap.enabled=false \
    prepare_data.py && \
    echo "Putting data to hdfs" && \
    hdfs dfs -put data / && \
    hdfs dfs -ls /data && \
    # hdfs dfs -ls /indexer/data && \
    echo "done data preparation!"
