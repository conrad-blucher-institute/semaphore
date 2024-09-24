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
from DatabaseMigration.databaseMigrationUtility import KeywordType, DatabaseDeletionHelper
from sqlalchemy import Engine, MetaData, Table, delete, select, update
from sqlalchemy.dialects.postgresql import insert 
from sqlalchemy.sql import text


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """ This function updates the database to version 2.6 which adds user accounts
            for the API, Semaphore-Core, and the CBI team.

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        # Setting engine
        self.__engine = databaseEngine

        # Create and Set Users

        # Check that Users are there 
        
        # Return True if all users are there
    

    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database from 2.6.

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        # Setting engine
        self.__engine = databaseEngine

        # Check that Users are there 
        
        # Remove Users

        # Check that Users are gone

        # Return True
    

    def create_admin_user(self, user, password):
        """Creates a database user with superuser privileges and sets permissions."""
        with self.__engine.connect() as conn:
            # Create a new user with superuser privileges
            conn.execute(text(f"CREATE ROLE {user} WITH LOGIN SUPERUSER PASSWORD '{password}';"))
            # Grant connect on the database to the new user
            conn.execute(text(f"GRANT CONNECT ON DATABASE {database_name} TO {user};"))
            # Grant all permissions on all tables in schema public to the new user
            conn.execute(text(f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {user};"))
            # Grant all permissions on all sequences in public schema to the new user
            conn.execute(text(f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {user};"))
            conn.commit()


    def create_general_user(self, user, password):
        """Creates a database user with read only permissions."""
        with self.__engine.connect() as conn:
            # Create a new user
            conn.execute(text(f"CREATE USER {user} WITH PASSWORD '{password}';"))
            # Grant connect on the database to the new user
            conn.execute(text(f"GRANT CONNECT ON DATABASE {database_name} TO {user};"))
            # Grant SELECT permissions on all tables in schema public to the new user
            conn.execute(text(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {user};"))
            # Grant SELECT permissions on all sequences in public schema to the new user
            conn.execute(text(f"GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO {user};"))
            conn.commit()


    def create_core_user(self, user, password):
        """Creates a database user with read only permissions."""
        with self.__engine.connect() as conn:
            # Create a new user
            conn.execute(text(f"CREATE USER {user} WITH PASSWORD '{password}';"))
            # Grant connect on the database to the new user
            conn.execute(text(f"GRANT CONNECT ON DATABASE {database_name} TO {user};"))
            # Grant SELECT permissions on all tables in schema public to the new user
            conn.execute(text(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {user};"))
            # Grant SELECT permissions on all sequences in public schema to the new user
            conn.execute(text(f"GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO {user};"))
            conn.commit()


    def create_api_user(self, user, password):
        """Creates a database user with superuser privileges and sets permissions."""
        with self.__engine.connect() as conn:
            # Create a new user with superuser privileges
            conn.execute(text(f"CREATE ROLE {user} WITH LOGIN SUPERUSER PASSWORD '{password}';"))
            # Grant connect on the database to the new user
            conn.execute(text(f"GRANT CONNECT ON DATABASE {database_name} TO {user};"))
            # Grant all permissions on all tables in schema public to the new user
            conn.execute(text(f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {user};"))
            # Grant all permissions on all sequences in public schema to the new user
            conn.execute(text(f"GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {user};"))
            conn.commit()