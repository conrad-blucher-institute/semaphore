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
import yaml
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

        # Check if all environment variables are set
        if not self.__check_env_variables():
            return False

        # Read user credentials from environment variables
        api_user = os.getenv('API_POSTGRES_USER')
        api_password = os.getenv('API_POSTGRES_PASSWORD')
        
        core_user = os.getenv('CORE_POSTGRES_USER')
        core_password = os.getenv('CORE_POSTGRES_PASSWORD')
        
        general_user = os.getenv('GENERAL_POSTGRES_USER')
        general_password = os.getenv('GENERAL_POSTGRES_PASSWORD')
        

        # Create and Set Users
        self.__create_api_user(api_user, api_password)
        self.__create_core_user(core_user, core_password)
        self.__create_general_user(general_user, general_password)

        # Check if users were added successfully
        if self.__check_user_exists(api_user) and self.__check_user_exists(core_user) and self.__check_user_exists(general_user):
            # Update the yml file so that the different accounts are actually being used
            self.__update_yml_file()
            # Return True
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

        # Check if all environment variables are set
        if not self.__check_env_variables():
            return False

        # Read users to Rollback
        api_user = os.getenv('API_POSTGRES_USER')
        core_user = os.getenv('CORE_POSTGRES_USER')
        general_user = os.getenv('GENERAL_POSTGRES_USER')

        # Remove Users
        self.__remove_user(api_user)
        self.__remove_user(core_user)
        self.__remove_user(general_user)

        # Check if users were removed successfully
        if not (self.__check_user_exists(api_user) or self.__check_user_exists(core_user) or self.__check_user_exists(general_user)):
            # Update the yml file so that the one account we had before is being used
            self.__rollback_yml_file()
            # Return True
            return True
        else:
            return False
        
    
    def __check_env_variables(self)-> bool:
        """Checks if all required environment variables are set."""
        
        # List of required environment variables
        required_vars = [
            'API_POSTGRES_USER',
            'API_POSTGRES_PASSWORD',
            'CORE_POSTGRES_USER',
            'CORE_POSTGRES_PASSWORD',
            'GENERAL_POSTGRES_USER',
            'GENERAL_POSTGRES_PASSWORD',
            'POSTGRES_DB'
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
        print("All required environment variables are set.")
        return True


    def __create_general_user(self, user: str, password: str):
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


    def __create_api_user(self, user: str, password: str):
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


    def __create_core_user(self, user: str, password: str):
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


    def __check_user_exists(self, user: str) -> bool:
        """Checks if a user exists in the PostgreSQL database.

           :param: user - str - The name of the user to check.
           :return: bool - indicating if the user exists
        """
        with self.__engine.connect() as conn:
            result = conn.execute(text(f"SELECT 1 FROM pg_roles WHERE rolname = '{user}';"))
            return result.fetchone() is not None


    def __update_yml_file(self):
        """ Updates the .yml file with new user details.
        """
        # Getting yml path
        yml_path = os.path.join(os.path.dirname(__file__), 'docker-compose.yml')

        # Opening the yml file
        with open(yml_path, 'r') as file:
            config = yaml.safe_load(file)

        # Modify the container details to use correct connection strings
        if 'services' in config and 'core' in config['services']:
            service = config['services']['core']
            if 'environment' in service:
                # Update the DB_LOCATION_STRING
                for i, env_var in enumerate(service['environment']):
                    if env_var.startswith('DB_LOCATION_STRING'):
                        service['environment'][i] = 'DB_LOCATION_STRING=${CORE_DB_LOCATION_STRING}'
                        break
        
        if 'services' in config and 'api' in config['services']:
            service = config['services']['api']
            if 'environment' in service:
                # Update the DB_LOCATION_STRING
                for i, env_var in enumerate(service['environment']):
                    if env_var.startswith('DB_LOCATION_STRING'):
                        service['environment'][i] = 'DB_LOCATION_STRING=${API_DB_LOCATION_STRING}'
                        break

        # Write back the updated config
        with open(yml_path, 'w') as file:
            yaml.dump(config, file)


    def __remove_user(self, user: str):
        """ Removes a user from the database if it exists.
        
            :param: user - str - The username of the account we are removing.
        """
        with self.__engine.connect() as conn:
            conn.execute(text(f"DROP ROLE IF EXISTS {user};"))
            conn.commit()
            

    def __rollback_yml_file(self):
        """ Updates the .yml file with new user details.
        """
        # Getting yml path
        yml_path = os.path.join(os.path.dirname(__file__), 'docker-compose.yml')
        
        # Opening the yml file
        with open(yml_path, 'r') as file:
            config = yaml.safe_load(file)

        # Modify the container details to use correct connection strings
        if 'services' in config and 'core' in config['services']:
            service = config['services']['core']
            if 'environment' in service:
                # Update the DB_LOCATION_STRING
                for i, env_var in enumerate(service['environment']):
                    if env_var.startswith('DB_LOCATION_STRING'):
                        service['environment'][i] = 'DB_LOCATION_STRING=${DB_LOCATION_STRING}'
                        break
        
        if 'services' in config and 'api' in config['services']:
            service = config['services']['api']
            if 'environment' in service:
                # Update the DB_LOCATION_STRING
                for i, env_var in enumerate(service['environment']):
                    if env_var.startswith('DB_LOCATION_STRING'):
                        service['environment'][i] = 'DB_LOCATION_STRING=${DB_LOCATION_STRING}'
                        break

        # Write back the updated config
        with open(yml_path, 'w') as file:
            yaml.dump(config, file, default_flow_style=False)