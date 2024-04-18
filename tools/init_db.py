# -*- coding: utf-8 -*-
#init_db.py
#----------------------------------
# Created By: Team
# Created Date: 2/07/2023
# version 2.0
#----------------------------------
"""This file instantiates a db schema over a db connection.
 """ 
#----------------------------------
# 
from os import getenv
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists

load_dotenv()

def main():
    # Load database location string
    DB_LOCATION_STRING = os.getenv('DB_LOCATION_STRING')

    # Create the database engine
    engine = create_engine(DB_LOCATION_STRING)

    # Check if the database exists
    dbExists = database_exists(engine.url)

    # If the database doesn't exist
    if dbExists == 0: 
        #Instantiate the base version of the database
        
    # If the database does exist
    elif dbExists ==1: 

if __name__ == "__main__":
    main()


