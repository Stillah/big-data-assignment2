#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install --upgrade pip setuptools wheel

echo "requirements.txt content:"
cat requirements.txt
pip install -r requirements.txt  

# Package the virtual env.
# rm .venv.tar.gz
venv-pack -o .venv.tar.gz

# Collect data
bash prepare_data.sh

echo "Success (?)"
# # Run the indexer
bash index.sh /data

# # Run the ranker
bash search.sh "please work man"

bash search.sh "I love big data"

bash search.sh "and this assignment"
