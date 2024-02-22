#!/usr/bin/env python
#HEALTHCHECK_CORE.PY
#-------------------------------
# Created By : Anointiyae Beasley
# Created Date: 2/21/2024
# version 1.0
#-------------------------------
""" This file connects to the database and runs a test query as a healthcheck for semahpore core
 """ 
#-------------------------------
# 
#
from sqlalchemy import create_engine, Select, text

import os
from dotenv import load_dotenv

try:
    # Load environment variables from .env file
    load_dotenv()

    DB_LOCATION_STRING = os.getenv('DB_LOCATION_STRING')
    engine = create_engine(DB_LOCATION_STRING)
    
    #Connecting to the database to execute a test query
    with engine.connect() as conn:
        cursor = conn.execute(text("SELECT 1"))
        result = cursor.fetchall()
        
    #Testing whether the result gave the correct information
    tupleList = [(1,)]    
    if result == tupleList:  
        exit(0)
    
    else:    
        exit(1)
  
except Exception as e: 
    exit(1)
 
   
   


