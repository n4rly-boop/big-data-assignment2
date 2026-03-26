#!/bin/bash

if [ -z "$1" ]; then
    echo "usage: bash search.sh \"query\""
    exit 1
fi

echo "searching for: $1" >&2

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

spark-submit \
    --master yarn \
    --archives /app/.venv.tar.gz#.venv \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
    /app/query.py "$1"
