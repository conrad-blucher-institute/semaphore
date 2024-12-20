# -*- coding: utf-8 -*-
#init_db_users.py
#----------------------------------
# Created By: Savannah Stephenson
# Created Date: 10/09/2024
#----------------------------------
""" This is a tools script used to add three user accounts to the semaphore database
    one for the API, one for Semaphore-Core, and one for read only team members. 

    To use: 
    docker exec semaphore-core python3 tools/init_db_users.py --api-user "api_user" --api-password "api_password" --core-user "core_user" --core-password "core_password" --general-user "general_user" --general-password "general_password"

 """ 
#----------------------------------
# 
#
#Imports
from sqlalchemy import Engine, create_engine
from sqlalchemy.sql import text
from dotenv import load_dotenv
import argparse
import os

# load database name from .env file
load_dotenv()

def create_general_user(engine: Engine, user: str, password: str):
    """ Creates a database user with read only permissions.

        :param: user - str - The username for the user account.
        :param: password - str - The password for the user account.
    """
    with engine.begin() as conn:
        # Create a new user
        conn.execute(text("CREATE USER :user WITH PASSWORD :password"),
                     {"user": user, "password": password})
        # Grant connect on the database to the new user
        conn.execute(text("GRANT CONNECT ON DATABASE :db TO :user"),
                     {"db": os.getenv('POSTGRES_DB'), "user": user})
        # Grant SELECT permissions on all tables in schema public to the new user
        conn.execute(text("GRANT SELECT ON ALL TABLES IN SCHEMA public TO :user"),
                     {"user": user})
        # Grant SELECT permissions on all sequences in public schema to the new user
        conn.execute(text("GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO :user"),
                     {"user": user})
        # Ensure that any future tables and sequences created in the 'public' schema also grant appropriate permissions
        conn.execute(text("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO :user"),
                     {"user": user})
        conn.execute(text("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO :user"),
                     {"user": user})


def create_api_user(engine: Engine, user: str, password: str):
    """ Creates a database user with read and insert permissions.
    
        :param: user - str - The username for the user account.
        :param: password - str - The password for the user account.
    """
    with engine.begin() as conn:
        conn.execute(text("CREATE USER :user WITH PASSWORD :password"),
                     {"user": user, "password": password})
        conn.execute(text("GRANT CONNECT ON DATABASE :db TO :user"),
                     {"db": os.getenv('POSTGRES_DB'), "user": user})
        conn.execute(text("GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO :user"),
                     {"user": user})
        conn.execute(text("GRANT SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO :user"),
                     {"user": user})
        conn.execute(text("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE ON TABLES TO :user"),
                     {"user": user})
        conn.execute(text("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, UPDATE ON SEQUENCES TO :user"),
                     {"user": user})


def create_core_user(engine: Engine, user: str, password: str):
    """ Creates a database user with superuser privileges and sets permissions.

        :param: user - str - The username for the user account.
        :param: password - str - The password for the user account.
    """
    with engine.begin() as conn:
        conn.execute(text("CREATE ROLE :user WITH LOGIN SUPERUSER PASSWORD :password"),
                     {"user": user, "password": password})
        conn.execute(text("GRANT CONNECT ON DATABASE :db TO :user"),
                     {"db": os.getenv('POSTGRES_DB'), "user": user} )
        conn.execute(text("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO :user"),
                     {"user": user} )
        conn.execute(text("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO :user"),
                     {"user": user})


def check_user_exists(engine: Engine, user: str) -> bool:
    """Checks if a user exists in the PostgreSQL database.

        :param: user - str - The name of the user to check.
        :return: bool - indicating if the user exists
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT 1 FROM pg_roles WHERE rolname = '{user}';"))
        return result.fetchone() is not None

def parse_args():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Database Accounts Script")
    parser.add_argument('--api-user', required=True, help='API database username')
    parser.add_argument('--api-password', required=True, help='API database password')
    parser.add_argument('--core-user', required=True, help='Core database username')
    parser.add_argument('--core-password', required=True, help='Core database password')
    parser.add_argument('--general-user', required=True, help='General database username')
    parser.add_argument('--general-password', required=True, help='General database password')
    
    return parser.parse_args()

def main():
    """ The main function of init_db_users.py creates a database engine and 
        uses it to make differnt database users. 
    """
    # Get arguments
    args = parse_args()

    # Load database location string
    DB_LOCATION_STRING = os.getenv('DB_LOCATION_STRING')

    # Create the database engine
    engine = create_engine(DB_LOCATION_STRING)

    # Create and Set Users
    create_api_user(engine, args.api_user, args.api_password)
    create_core_user(engine, args.core_user, args.core_password)
    create_general_user(engine, args.general_user, args.general_password)

     # Print connection strings
    print(f"API User connection string: postgresql+psycopg2://{args.api_user}:password_here@host.docker.internal:5435/semaphoredb")
    print(f"Core User connection string: postgresql+psycopg2://{args.core_user}:password_here@host.docker.internal:5435/semaphoredb")
    print(f"General User connection string: postgresql+psycopg2://{args.general_user}:password_here@host.docker.internal:5435/semaphoredb")

    # Check if users were added successfully
    if check_user_exists(engine, args.api_user) and check_user_exists(engine, args.core_user) and check_user_exists(engine, args.general_user):
        return True
    else:
        return False 
    
if __name__ == "__main__":
    main()