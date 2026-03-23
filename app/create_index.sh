#!/bin/bash
set -e

INPUT_PATH=${1:-/input/data}
TMP_DIR=/tmp/indexer
DOC_STATS_OUT=$TMP_DIR/doc_stats
INDEX_OUT=$TMP_DIR/inverted_index

echo "creating index from $INPUT_PATH"

STREAMING_JAR=$(find $HADOOP_HOME -name "hadoop-streaming*.jar" | head -1)

# clean previous runs
hdfs dfs -rm -r -f $DOC_STATS_OUT
hdfs dfs -rm -r -f $INDEX_OUT

# job 1: doc stats (single reducer for correct stats)
hadoop jar $STREAMING_JAR \
    -D mapreduce.job.reduces=1 \
    -input $INPUT_PATH \
    -output $DOC_STATS_OUT \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -file /app/mapreduce/mapper1.py \
    -file /app/mapreduce/reducer1.py

echo "job 1 done"
hdfs dfs -cat $DOC_STATS_OUT/part-* | tail -3

# job 2: inverted index
hadoop jar $STREAMING_JAR \
    -input $INPUT_PATH \
    -output $INDEX_OUT \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -file /app/mapreduce/mapper2.py \
    -file /app/mapreduce/reducer2.py

echo "job 2 done"
hdfs dfs -cat $INDEX_OUT/part-* | head -5

echo "index creation complete"
