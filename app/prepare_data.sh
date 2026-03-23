#!/bin/bash
set -e

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

# create text files from parquet
hdfs dfs -put -f a.parquet /
spark-submit --driver-memory 2g prepare_data.py
echo "text files created"

# upload to hdfs
hdfs dfs -rm -r -f /data
hdfs dfs -put data /
hdfs dfs -ls /data
echo "data uploaded to hdfs"

# transform to tab separated for mapreduce
hdfs dfs -rm -r -f /input/data
spark-submit --driver-memory 2g transform_data.py
hdfs dfs -ls /input/data
echo "done data preparation"
