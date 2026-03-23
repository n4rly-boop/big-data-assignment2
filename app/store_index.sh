#!/bin/bash
set -e
echo "storing index in cassandra"
source .venv/bin/activate
python3 /app/store_index_helper.py
echo "index stored"
