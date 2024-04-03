# -*- coding: utf-8 -*-
#init_db.py
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


if __name__ == "__main__":
    main()


