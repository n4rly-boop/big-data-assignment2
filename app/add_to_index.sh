#!/bin/bash
set -e

if [ -z "$1" ]; then
    echo "usage: bash add_to_index.sh <path_to_txt_file>"
    exit 1
fi

echo "adding document: $1"
source .venv/bin/activate
python3 /app/add_to_index_helper.py "$1"
echo "done"
