# -*- coding: utf-8 -*-
#init_db_users.py
#----------------------------------
# Created By: Savannah Stephenson
# Created Date: 10/09/2024
# Version 1.0
#----------------------------------
"""This is a database migration script that will update to version
    2.5 of the database. The intended change is adding three user accounts
    one for the API, one for Semaphore-Core, and one for Semaphore team members. 

    Note that changes to the .env file have been done to facilitate this
    database migration. The connection strings for all accounts must be set to 
    the default connection string before running.
 """ 
#----------------------------------
# 
#
#Imports
from sqlalchemy import Engine, create_engine
from sqlalchemy.sql import text
from dotenv import load_dotenv
import dotenv
from pathlib import Path
import os

# load variables from .env file
load_dotenv()

def main():
    """ Main
    """
    # Load database location string
    DB_LOCATION_STRING = os.getenv('DB_LOCATION_STRING')

    # Create the database engine
    engine = create_engine(DB_LOCATION_STRING)

    # Check if all environment variables are set
    if not check_env_variables():
        return False

    # Read user credentials from environment variables
    api_user = os.getenv('API_POSTGRES_USER')
    api_password = os.getenv('API_POSTGRES_PASSWORD')
    
    core_user = os.getenv('CORE_POSTGRES_USER')
    core_password = os.getenv('CORE_POSTGRES_PASSWORD')
    
    general_user = os.getenv('GENERAL_POSTGRES_USER')
    general_password = os.getenv('GENERAL_POSTGRES_PASSWORD')
    

    # Create and Set Users
    create_api_user(api_user, api_password)
    create_core_user(core_user, core_password)
    create_general_user(general_user, general_password)

    # Check if users were added successfully
    if check_user_exists(api_user) and check_user_exists(core_user) and check_user_exists(general_user):
        return True
    else:
        return False 
    

def check_env_variables(self)-> bool:
    """Checks if all required environment variables are set."""
    
    # List of required environment variables
    required_vars = [
        'API_POSTGRES_USER',
        'API_POSTGRES_PASSWORD',
        'CORE_POSTGRES_USER',
        'CORE_POSTGRES_PASSWORD',
        'GENERAL_POSTGRES_USER',
        'GENERAL_POSTGRES_PASSWORD',
        'POSTGRES_DB', 
        'DB_LOCATION_STRING',
        'NEW_CORE_DB_LOCATION_STRING',
        'NEW_API_DB_LOCATION_STRING',
        'NEW_GENERAL_DB_LOCATION_STRING'
    ]
    
    missing_vars = []
    
    # Check each environment variable
    for var in required_vars:
        if os.getenv(var) is None:
            missing_vars.append(var)
    
    # If there are missing variables, raise an error or print a message
    if missing_vars:
        print(f"Error: The following environment variables are missing: {', '.join(missing_vars)}")
        return False
    
    # If all variables are set, return True
    return True


def create_general_user(self, user: str, password: str):
    """ Creates a database user with read only permissions.

        :param: user - str - The username for the user account.
        :param: password - str - The password for the user account.
    """
    with self.__engine.begin() as conn:
        # Create a new user
        conn.execute(text(f"CREATE USER {user} WITH PASSWORD '{password}';"))
        # Grant connect on the database to the new user
        conn.execute(text(f"GRANT CONNECT ON DATABASE {os.getenv('POSTGRES_DB')} TO {user};"))
        # Grant SELECT permissions on all tables in schema public to the new user
        conn.execute(text(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {user};"))
        # Grant SELECT permissions on all sequences in public schema to the new user
        conn.execute(text(f"GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO {user};"))
        # Ensure that any future tables and sequences created in the 'public' schema also grant appropriate permissions
        conn.execute(text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {user};"))
        conn.execute(text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO {user};"))


def create_api_user(self, user: str, password: str):
    """ Creates a database user with read and insert permissions.
    
        :param: user - str - The username for the user account.
        :param: password - str - The password for the user account.
    """
    with self.__engine.begin() as conn:
        # Create a new user
        conn.execute(text(f"CREATE USER {user} WITH PASSWORD '{password}';"))
        # Grant connect on the database to the new user
        conn.execute(text(f"GRANT CONNECT ON DATABASE {os.getenv('POSTGRES_DB')} TO {user};"))
        # Grant SELECT, INSERT, and UPDATE permissions on all tables in the 'public' schema
        conn.execute(text(f"GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO {user};"))
        # Grant SELECT and UPDATE permissions on all sequences (for auto-increment columns) in the 'public' schema
        conn.execute(text(f"GRANT SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO {user};"))
        # Ensure that any future tables and sequences created in the 'public' schema also grant appropriate permissions
        conn.execute(text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO {user};"))
        conn.execute(text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, UPDATE ON SEQUENCES TO {user};"))


def create_core_user(self, user: str, password: str):
    """ Creates a database user with superuser privileges and sets permissions.

        :param: user - str - The username for the user account.
        :param: password - str - The password for the user account.
    """
    with self.__engine.begin() as conn:
        # Create a new user with superuser privileges
        conn.execute(text(f"CREATE ROLE {user} WITH LOGIN SUPERUSER PASSWORD '{password}';"))
        # Grant connect on the database to the new user
        conn.execute(text(f"GRANT CONNECT ON DATABASE {os.getenv('POSTGRES_DB')} TO {user};"))
        # Grant all permissions on all tables in schema public to the new user
        conn.execute(text(f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {user};"))
        # Grant all permissions on all sequences in public schema to the new user
        conn.execute(text(f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {user};"))


def check_user_exists(self, user: str) -> bool:
    """Checks if a user exists in the PostgreSQL database.

        :param: user - str - The name of the user to check.
        :return: bool - indicating if the user exists
    """
    with self.__engine.connect() as conn:
        result = conn.execute(text(f"SELECT 1 FROM pg_roles WHERE rolname = '{user}';"))
        return result.fetchone() is not None
