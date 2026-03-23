#!/bin/bash
set -e

INPUT_PATH=${1:-/input/data}
echo "running indexing pipeline"

bash /app/create_index.sh $INPUT_PATH
bash /app/store_index.sh

echo "indexing done"
