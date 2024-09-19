# -*- coding: utf-8 -*-
#2_6_DatabaseMigration.py
#----------------------------------
# Created By: Savannah Stephenson
# Created Date: 09/19/2024
# Version 1.0
#----------------------------------
"""This is a database migration script that will update to version
    2.6 of the database. The intended change is adding user accounts
    for the API, Semaphore-Core, and Semaphore team members. 
 """ 
#----------------------------------
# 
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from DatabaseMigration.databaseMigrationUtility import KeywordType, DatabaseDeletionHelper
from sqlalchemy import Engine, MetaData, Table, delete, select, update
from sqlalchemy.dialects.postgresql import insert 
import csv

#Constants
CSV_FILE_PATHS = './tools/DatabaseMigration/2_6/init_data'


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 2.6 which adds user accounts.

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        # Setting engine
        self.__engine = databaseEngine

        # Reflect the tables from the database that we want to add data too
        metadata = MetaData()
        metadata.reflect(bind=databaseEngine)
        
        ref_dataSeries = metadata.tables['ref_dataSeries']

        # Read in the data based off of csv files
        new_data = self.__readInitCSV('newRows.csv')
        old_data = self.__readInitCSV('oldRows.csv')

        results = self.__update_ref_data(old_data, new_data, ref_dataSeries)

        return True if len(results) == 3 else False
    
    def __readInitCSV(self, csvFileName: str) -> list:
        """This function reads in a CSV file with the data needed for the initialization 
            of the database

            :param csvFileName: str - CSV file name
            :return: list of dictionaries
        """
        csvFilePath = f'{CSV_FILE_PATHS}/{csvFileName}'
        dictionaryList = []
        with open(csvFilePath, mode = 'r') as infile:
            csvDict = csv.DictReader(infile)
            for dictionary in csvDict:
                dictionaryList.append(dictionary)

        return dictionaryList
    
    def __update_ref_data(self, old: list[dict], new: list[dict], table: Table) -> list[tuple]:
        """This method inserts reference rows

            :param old: the old rows to update.
            :param new: The new rows to update.
            :return Series - SQLALCHEMY tupleish rows
            NOTE:: {"code": None, "displayName": None, "notes": None}
            NOTE:: Old and new rows MUST be in the same order!
        """

        results = []
        with self.__engine.connect() as conn:
            for old, new in zip(old, new):
                cursor = conn.execute(update(table)
                                    .where(table.c.code == old['code'])
                                    .where(table.c.displayName == old['displayName'])
                                    .where(table.c.notes == old['notes'])
                                    .returning(table)
                                    .values(new)
                                    )
                results.append(cursor.fetchall())
            conn.commit() # Commit only after every update is successful
        return results

    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database from 2.6.

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        # Setting engine
        self.__engine = databaseEngine

        # Reflect the tables from the database that we want to add data too
        metadata = MetaData()
        metadata.reflect(bind=databaseEngine)
        
        ref_dataSeries = metadata.tables['ref_dataSeries']

        # Read in the data based off of csv files, but we swap old to new and new to old as this is a rollback
        old_data = self.__readInitCSV('newRows.csv')
        new_data = self.__readInitCSV('oldRows.csv')

        results = self.__update_ref_data(old_data, new_data, ref_dataSeries)

        return True if len(results) == 3 else False


# -*- coding: utf-8 -*-
#init_db_users.py
#----------------------------------
# Created By: Savannah Stephenson
# Created Date: 04/03/2024
# version 1.0
#----------------------------------
"""This file adds user accounts to the database based off of passed arguments. 
 """ 
#----------------------------------
# 
#
import sys
sys.path.append('/app/src')
from SeriesStorage.ISeriesStorage import series_storage_factory
from utility import construct_true_path

import csv
from os import getenv
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import argparse

load_dotenv()

def create_admin_user_and_set_permissions(engine, user, password, database_name):
    """Creates a database user with superuser privileges and sets permissions."""
    with engine.connect() as conn:
        # Create a new user with superuser privileges
        conn.execute(text(f"CREATE ROLE {user} WITH LOGIN SUPERUSER PASSWORD '{password}';"))
        # Grant connect on the database to the new user
        conn.execute(text(f"GRANT CONNECT ON DATABASE {database_name} TO {user};"))
        # Grant all permissions on all tables in schema public to the new user
        conn.execute(text(f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {user};"))
        # Grant all permissions on all sequences in public schema to the new user
        conn.execute(text(f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {user};"))
        conn.commit()

def create_civ_user_and_set_permissions(engine, user, password, database_name):
    """Creates a database user with read only permissions."""
    with engine.connect() as conn:
        # Create a new user
        conn.execute(text(f"CREATE USER {user} WITH PASSWORD '{password}';"))
        # Grant connect on the database to the new user
        conn.execute(text(f"GRANT CONNECT ON DATABASE {database_name} TO {user};"))
        # Grant SELECT permissions on all tables in schema public to the new user
        conn.execute(text(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {user};"))
        # Grant SELECT permissions on all sequences in public schema to the new user
        conn.execute(text(f"GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO {user};"))
        conn.commit()

def main():

    #creating argument parser
    parser = argparse.ArgumentParser(
                    prog='init_user_account',
                    description='Add account to the database based off passed arguments.',
                    epilog='End Help')
   
    #adding argument types to the parser
    parser.add_argument("-t", "--type", type=str, required=True,
                       help= "The type of account you would like to make.")
    
    parser.add_argument("-u", "--username", type=str, required=True,
                       help= "The username for the account you want to make.")
    
    parser.add_argument("-p", "--password", type=str, required=True,
                       help= "The password for the account you want to make.")

    #parsing arguments
    args = parser.parse_args()

    if args.type == 'admin': 
        # Database user and permissions
        admin_connection_string = getenv('DB_LOCATION_STRING') 
        engine = create_engine(admin_connection_string)
        user = args.username
        password = args.password
        database_name = getenv('POSTGRES_DB')
        create_admin_user_and_set_permissions(engine, user, password, database_name)
    elif args.type == 'civ':
        # Database user and permissions
        admin_connection_string = getenv('DB_LOCATION_STRING') 
        engine = create_engine(admin_connection_string)
        user = args.username
        password = args.password
        database_name = getenv('POSTGRES_DB')
        create_civ_user_and_set_permissions(engine, user, password, database_name)
    else: 
        print(f'A valid account type was not provided. The valid types of accounts are admin and civ.')




