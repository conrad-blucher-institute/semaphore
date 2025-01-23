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
from os import path
from os import listdir, getcwd, getenv
from dotenv import load_dotenv
from sqlalchemy import create_engine, Engine, inspect, Table, MetaData, select, update
from importlib import import_module
from datetime import datetime
from json import load

load_dotenv()
TARGET_VERSION_FILEPATH = './tools/DatabaseMigration/target_version.json'
DATABASE_MIGRATION_MODULE_PATH= './tools/DatabaseMigration'


class Version():
    def __init__(self, version_str: str, separator: str = '_'):
        major, minor = map(int, version_str.split(separator))
        self.major = major
        self.minor = minor

    @classmethod
    def from_dot_separator(cls, version_str: str):
        return cls(version_str, separator='.')

    def __lt__(self, other):
        if self.major == other.major:
            return self.minor < other.minor
        return self.major < other.major

    def __gt__(self, other):
        if self.major == other.major:
            return self.minor > other.minor
        return self.major > other.major

    def __eq__(self, other):
        return self.major == other.major and self.minor == other.minor

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return f"{self.major}.{self.minor}"

    

def get_current_database_version(engine: Engine) -> Version:
    """This function get's the current version of the database from the database version table in the database.
            :param engine: Engine -The engine to connect to the database
    """
    #check if the database exists
    inspector = inspect(engine)
    dbExists = inspector.has_table('inputs')
    versionTableExists = inspector.has_table('deployed_database_version')

    if not dbExists:
        return Version('0_0')
    if not versionTableExists:
        return Version('1_0')
    
    #query the version table and return database version
    metadata_obj = MetaData()
    version_table = Table("deployed_database_version", metadata_obj, autoload_with=engine)

    with engine.begin() as connection:
        stmt = select(version_table.c.versionNumber)
        cursor = connection.execute(stmt)
        version = cursor.fetchall()[0][0]

    return Version.from_dot_separator(version)
    
def set_current_database_version(engine: Engine, version: float, description='') -> None:
    """This function sets the current database version to the passed version number
       in the database version database table. 
            :param engine: Engine -The engine to connect to the database with
            :param version: float -The version to update the database to
            :param description: str -A description of the version of the database
    """

    # The deployed_database_version isn't in the db till 2.0
    if version == Version.from_dot_separator('1.0'):
        return

    #set up table information
    metadata_obj = MetaData()
    version_table = Table("deployed_database_version", metadata_obj, autoload_with=engine)

    with engine.begin() as connection:
        stmt = (update(version_table)
                       .values({
                           version_table.c.versionNumber: str(version.major) + '.' + str(version.minor), 
                           version_table.c.migrationTime: datetime.utcnow(), 
                           version_table.c.versionNotes: description
                       }))
        connection.execute(stmt)
        connection.commit()

def get_target_version_info()-> tuple[Version, str]:
    """ This function returns the target version information by reading from the target
        version json file. 
            :return target_version, target_version_description: tuple[int,str] -The version number and description
    """
    # Check that target version json exists
    if not path.exists(TARGET_VERSION_FILEPATH):
        raise FileNotFoundError(f'{TARGET_VERSION_FILEPATH} not found!')
    
    with open(TARGET_VERSION_FILEPATH) as version_info:
        json = load(version_info)
        target_version = Version.from_dot_separator(json.get('Target Version'))
        target_version_description = json.get('Description')
        if not target_version or not target_version_description:
            raise KeyError(f'{TARGET_VERSION_FILEPATH} missing information.')
        
        return target_version, target_version_description

def find_next_version_idx(current_version: float, is_update: bool, version_list: list) -> int: 
    """This function returns the version number that is the next sequential step in
       migrating the database. 
            :param current_version: float -The current version that the database is on, read from database version table
            :param is_update: bool -The indicator if we are updating or rolling back (this tells us if we should look to the right or the left in the list)
            :param version_list: list -A list of all the database update options in semaphore
            :return index: int - The index of the next version
    """
    #search list for current version 
    current_version_idx = version_list.index(current_version)

    if is_update:
        #return the number to the right
        return current_version_idx + 1
    else:
        #return the number to the left
        return current_version_idx - 1

def create_version_lists() -> tuple[list[str], list[tuple[int]]]:
    '''
        The version directories have to be saved with _ instead of .
        This method processes the DatabaseMigration dir to unpack all the
        float version such that they can be mathematically compared.
        String names are also returned to be used to import the migration modules
    '''
    base_path = path.join(getcwd(), DATABASE_MIGRATION_MODULE_PATH)
    # There is no 0_0 dir but that is the first theoretical version of the DB

    names = ['0_0'] + [entry for entry in listdir(base_path) if path.isdir(path.join(base_path, entry))]
    versions = [Version(name) for name in names]
    
    # Zips the two lists together, sorts them, unzips them
    versions, names = zip(*sorted(zip(versions, names)))
    return names, versions


def main():
    """The main function of migrate_db.py creates the database engine and passes
       it through the logic to update or rollback the database. 
    """
    print('Beginning Database Migration')

    # Load database location string
    DB_LOCATION_STRING = getenv('DB_LOCATION_STRING')

    # Create the database engine
    engine = create_engine(DB_LOCATION_STRING)

    current_version = get_current_database_version(engine)
    
    # Read the target version of the database from the static file
    target_version, target_desc = get_target_version_info()

    # Create list of version folders inside of DatabaseMigration folder
    version_names, version_values = create_version_lists()
    
    # Determine if updating or rolling back
    is_update = target_version > current_version
            
    # Check the current version of the database against the target version
    while (current_version != target_version):
        
        # Find the next version to update to
        next_version_idx = find_next_version_idx(current_version, is_update, version_values)
        next_version_value = version_values[next_version_idx]
        next_version_name = version_names[next_version_idx]        

        # Call method to update to next version
        if is_update:
            # Dynamically inject the next version's migration class
            migrator = getattr(import_module(f'DatabaseMigration.{next_version_name}.{next_version_name}_DatabaseMigration'), 'Migrator')()

            print(f'Updating Version from {current_version} to {next_version_value}')
            success = migrator.update(engine)
        else:
            # Dynamically inject the next version's migration class
            current_version_name = version_names[next_version_idx + 1] # The rollback we want to run is on the current versions script
            migrator = getattr(import_module(f'DatabaseMigration.{current_version_name}.{current_version_name}_DatabaseMigration'), 'Migrator')()

            print(f'Updating Version from {current_version} to {next_version_value}')
            success = migrator.rollback(engine)
        
        if not success:
            raise RuntimeError(f'ABORTING: {current_version} to {next_version_value} failed!')
        # Update version in database
        set_current_database_version(engine, next_version_value)  
        current_version = next_version_value

    # Update last time with description of database
    set_current_database_version(engine, target_version, target_desc)  

    # Log that migration has finished
    print(f'Fin!')     

if __name__ == "__main__":
    main()