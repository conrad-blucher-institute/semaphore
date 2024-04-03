# preprocess.py

import os

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Read the original SQL script
with open('entrypoint_creation.sql', 'r') as file:
    sql_script = file.read()

# Replace placeholders with actual values
sql_script = sql_script.replace('${CORE_USER}', os.getenv('CORE_USER'))
sql_script = sql_script.replace('${CORE_PASSWORD}', os.getenv('CORE_PASSWORD'))
sql_script = sql_script.replace('${API_USER}', os.getenv('CORE_USER'))
sql_script = sql_script.replace('${API_PASSWORD}', os.getenv('CORE_PASSWORD'))
sql_script = sql_script.replace('${POSTGRES_DB}', os.getenv('POSTGRES_DB'))

# Write the preprocessed SQL script to a new file
with open('preprocessed_entrypoint_creation.sql', 'w') as file:
    file.write(sql_script)
