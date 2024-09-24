# -*- coding: utf-8 -*-
#2_6_DatabaseMigration.py
#----------------------------------
# Created By: Savannah Stephenson
# Created Date: 09/19/2024
# Version 1.0
#----------------------------------
"""This is a database migration script that will update to version
    2.6 of the database. The intended change is adding three user accounts
    one for the API, one for Semaphore-Core, and one for Semaphore team members. 
 """ 
#----------------------------------
# 
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from sqlalchemy import Engine
from sqlalchemy.sql import text
from dotenv import load_dotenv
import os

# load variabesl from .env file
load_dotenv()

class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """ This function updates the database to version 2.6 which adds user accounts
            for the API, Semaphore-Core, and the CBI team.

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        # Setting engine
        self.__engine = databaseEngine

        # Read user credentials from environment variables
        api_user = os.getenv('API_USER')
        api_password = os.getenv('API_PASSWORD')
        
        core_user = os.getenv('CORE_USER')
        core_password = os.getenv('CORE_PASSWORD')
        
        general_user = os.getenv('GENERAL_USER')
        general_password = os.getenv('GENERAL_PASSWORD')

        # Create and Set Users
        self.__create_api_user(api_user, api_password)
        self.__create_core_user(core_user, core_password)
        self.__create_general_user(general_user, general_password)

        # Check if users were added successfully
        if self.__check_user_exists(api_user) and self.__check_user_exists(core_user) and self.__check_user_exists(general_user):
            return True
        else:
            return False 
    

    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database from 2.6.

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        # Setting engine
        self.__engine = databaseEngine

        # Read users to Rollback
        api_user = os.getenv('API_USER')
        core_user = os.getenv('CORE_USER')
        general_user = os.getenv('GENERAL_USER')

        # Remove Users
        self.__remove_user(api_user)
        self.__remove_user(core_user)
        self.__remove_user(general_user)

        # Check if users were removed successfully
        if not (self.__check_user_exists(api_user) or self.__check_user_exists(core_user) or self.__check_user_exists(general_user)):
            return True
        else:
            return False


    def __create_general_user(self, user: str, password: str):
        """ Creates a database user with read only permissions.

            :param: user - str - The username for the user account.
            :param: password - str - The password for the user account.
        """
        with self.__engine.connect() as conn:
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
            # Commit Changes
            conn.commit()


    def __create_api_user(self, user: str, password: str):
        """ Creates a database user with read and insert permissions.
        
            :param: user - str - The username for the user account.
            :param: password - str - The password for the user account.
        """
        with self.__engine.connect() as conn:
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
            # commit Changes
            conn.commit()


    def __create_core_user(self, user: str, password: str):
        """ Creates a database user with superuser privileges and sets permissions.

            :param: user - str - The username for the user account.
            :param: password - str - The password for the user account.
        """
        with self.__engine.connect() as conn:
            # Create a new user with superuser privileges
            conn.execute(text(f"CREATE ROLE {user} WITH LOGIN SUPERUSER PASSWORD '{password}';"))
            # Grant connect on the database to the new user
            conn.execute(text(f"GRANT CONNECT ON DATABASE {os.getenv('POSTGRES_DB')} TO {user};"))
            # Grant all permissions on all tables in schema public to the new user
            conn.execute(text(f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {user};"))
            # Grant all permissions on all sequences in public schema to the new user
            conn.execute(text(f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {user};"))
            conn.commit()


    def __check_user_exists(self, user: str) -> bool:
        """Checks if a user exists in the PostgreSQL database.

           :param: user - str - The name of the user to check.
           :return: bool - indicating if the user exists
        """
        with self.__engine.connect() as conn:
            result = conn.execute(text(f"SELECT 1 FROM pg_roles WHERE rolname = '{user}';"))
            return result.fetchone() is not None


    def __remove_user(self, user: str):
        """ Removes a user from the database if it exists.
        
            :param: user - str - The username of the account we are removing.
        """
        with self.__engine.connect() as conn:
            conn.execute(text(f"DROP ROLE IF EXISTS {user};"))
            conn.commit()