#!/bin/bash
set -e

# start ssh
service ssh restart

# start hadoop services
bash start-services.sh

# setup python env
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
venv-pack -f -o .venv.tar.gz

# prepare data
bash prepare_data.sh

# index documents
bash index.sh /input/data

# run some queries
echo ""
echo "========================================="
echo "  sample queries"
echo "========================================="

bash search.sh "world war history"
echo ""
bash search.sh "music album rock"
echo ""
bash search.sh "film movie director"
