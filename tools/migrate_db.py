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
from os import path
from dotenv import load_dotenv
from sqlalchemy import create_engine, Engine
from sqlalchemy_utils import database_exists
from importlib import import_module
from json import load

load_dotenv()
TARGET_VERSION_FILEPATH = './tools/DatabaseMigration/target_version.json'

def get_current_database_version(engine: Engine):
    """This function get's the current version of the database from the database version table in the database.
            :param engine: Engine -The engine to connect to the database
    """
    # This function should query the database to determine its current version.

    # Check if the database exists
    dbExists = database_exists(engine.url)
    # If the database doesn't exist
    if dbExists == 0: 
        # If the database doesn't exist then the current version is 0
        current_database_version = 0
        # Instantiate the base version of the database
        ## 

        #Update the current version of the database
        current_database_version = 1.0
    else: 
       # if the table exists return the current version
        #inspection = inspect(engine)
       # if inspection.has_table(''): existing_tables += 1
       # if the table doesn't exist return 1.0
       pass   
    pass

def set_current_database_version(engine: Engine, version: float, description=''):
    """This function sets the current database version to the passed version number
       in the database version database table. 
            :param engine: Engine -The engine to connect to the database with
            :param version: float -The version to update the database to
            :param description: str -A description of the version of the database
    """
    pass

def get_target_version_info()-> tuple[int, str]:
    """ This function returns the target version information by reading from the target
        version json file. 
            :return target_version, target_version_description: tuple[int,str] -The version number and description
    """
    # Check that target version json exists
    if not path.exists(TARGET_VERSION_FILEPATH):
        raise FileNotFoundError(f'{TARGET_VERSION_FILEPATH} not found!')
    
    with open(TARGET_VERSION_FILEPATH) as version_info:
        json = load(version_info)
        target_version = json.get('Target Version')
        target_version_description = json.get('Description')
        if not target_version or not target_version_description:
            raise KeyError(f'{TARGET_VERSION_FILEPATH} missing information.')
        
        return target_version, target_version_description

def find_next_version(current_version: float, is_update: bool, version_list: list) -> float: 
    """This function returns the version number that is the next sequential step in
       migrating the database. 
            :param current_version: float -The current version that the database is on, read from database version table
            :param is_update: bool -The indicator if we are updating or rolling back (this tells us if we should look to the right or the left in the list)
            :param version_list: list -A list of all the database update options in semaphore
            :return next_version: float -The next version of the database
    """
    #search list for current version 
    current_version_idx = version_list.index(current_version)

    if is_update:
        #return the number to the right
        return version_list[current_version_idx + 1]
    else:
        #return the number to the left
        return version_list[current_version_idx - 1]


def main():
    """The main function of migrate_db.py creates the database engine and passes
       it through the logic to update or rollback the database. 
    """
    print('Beginning Database Migration')

    # Load database location string
    DB_LOCATION_STRING = os.getenv('DB_LOCATION_STRING')

    # Create the database engine
    engine = create_engine(DB_LOCATION_STRING)

    # Fetch current database version 
    current_version = get_current_database_version()
    
    # Read the target version of the database from the static file
    target_version, target_desc = get_target_version_info()

    # Create list of version folders inside of DatabaseMigration folder
    path = os.path.join(os.getcwd(), 'DatabaseMigration')
    version_list = ['0.0'] + os.listdir(path)
    
    # Determine if updating or rolling back
    is_update = target_version > current_version
            
    # Check the current version of the database against the target version
    while (current_version != target_version):
        
        # Find the next version to update to
        next_version = find_next_version(current_version, is_update, version_list)
    
        # Dynamically inject the next version's migration class
        migrator = getattr(import_module(f'.DatabaseMigration.{next_version}.{next_version}_DatabaseMigration', 'tools'), 'Migrator')()

        # Call method to update to next version
        if is_update:
            print(f'Updating Version from {current_version} to {next_version}')
            migrator.update(engine)
        else:
            print(f'Updating Version from {current_version} to {next_version}')
            migrator.rollback(engine)
        
        # Update version in database
        set_current_database_version(engine, next_version)  

    # Update last time with description of database
    set_current_database_version(engine, next_version, target_desc)  

    # Log that migration has finished
    print(f'Finished ')     

if __name__ == "__main__":
    main()