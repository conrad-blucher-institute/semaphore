#!/bin/bash

# Run the preprocessing script
python scripts/preprocess.py

# Execute the preprocessed SQL script to create users and grant privileges
psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -f /scripts/preprocessed_script.sql

# Start PostgreSQL
exec docker-entrypoint.sh "$@"
