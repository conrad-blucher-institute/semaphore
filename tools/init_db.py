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
import os
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
        # If the database doesn't exist then the current version is 0
        current_database_version = 0
        # Instantiate the base version of the database
        ## Navigate to the 1.0 folder in the DatabaseMigration folder

        ## Run the python files inside of the 1.0 folder

        #Update the current version of the database
        current_database_version = 1.0
        
    # If the database does exist
    if dbExists ==1:
        # Read the current version of the database from the table in 
        # the database, if the table doesn't exist, then assume database
        # is at version 1.0

        # Read the target version of the database from the static file
        
        # Check the current version of the database against the target version
        
        # While target version != current version
        
        ## If the target is higher than current version update until versions match
        ### Navigate to first folder in DatabaseMigration that is larger than
        ### current version and run scripts inside. 
        
        ## If the target is lower than the current version rollback until versions match
        ### Navigate to first folder in DatabaseMigration that is smaller than
        ### current version and run scripts inside. 
        
        ## Update current version variable

        pass
        

if __name__ == "__main__":
    main()


