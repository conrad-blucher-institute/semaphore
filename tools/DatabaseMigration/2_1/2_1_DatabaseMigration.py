# -*- coding: utf-8 -*-
#2_1_DatabaseMigration.py
#----------------------------------
# Created By: Savannah Stephenson
# Created Date: 05/04/2024
# Version 1.0
#----------------------------------
"""This is a database migration script that will initialize version
    2.1 of the database. The change from version 2.0 to 2.1 is that
    we are adding rows to the locations reference table and the data
    source reference table. 
 """ 
#----------------------------------
# 
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from sqlalchemy import Engine, MetaData, Table, delete, select
from sqlalchemy.dialects.postgresql import insert
import csv

#Constants
CSV_FILE_PATHS = './tools/DatabaseMigration/2_1/init_data'


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 2.1 which adds rows to the locations reference 
           table and the data source reference table.

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        # Setting engine
        self.__engine = databaseEngine

        # Reflect the tables from the database that we want to add data too
        metadata = MetaData()
        metadata.reflect(bind=databaseEngine)
        self.ref_dataLocation = metadata.tables['ref_dataLocation']
        self.ref_dataSource = metadata.tables['ref_dataSource']

        # Insert the data based off of csv files
        self.insert_ref_dataLocation(self.readInitCSV('magnoliaLocations.csv'))
        self.insert_ref_dataSource(self.readInitCSV('semaphoreDataSource.csv'))
        
        return True
    
    def readInitCSV(self, csvFileName: str) -> list:
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
    
    def insert_ref_dataLocation(self, rows: list[dict]) -> list[tuple]:
        """This method inserts reference rows
            :param rows: A list of dictionaries. The dict can be found in NOTE 1
            :return Series - SQLALCHEMY tupleish rows
            NOTE:: {"code": None, "displayName": None, "notes": None, "latitude": None, "longitude": None}
        """
        with self.__engine.connect() as conn:
            cursor = conn.execute(insert(self.ref_dataLocation)
                                  .returning(self.ref_dataLocation)
                                  .values(rows)
                                  )
            result = cursor.fetchall()
            conn.commit()
            return result
        
    def insert_ref_dataSource(self, rows: list[dict]) -> list[tuple]:
        """This method inserts reference rows
            :param rows: A list of dictionaries. The dict can be found in NOTE 1
            :return Series - SQLALCHEMY tupleish rows
            NOTE:: {"code": None, "displayName": None, "notes": None}
        """
        
        with self.__engine.connect() as conn:
            cursor = conn.execute(insert(self.ref_dataSource)
                                  .returning(self.ref_dataSource)
                                  .values(rows)
                                  )
            result = cursor.fetchall()
            conn.commit()
        return result


    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database back to version 2.0 which involves removing the changes 
           associated with version 2.1 

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        # Setting engine
        self.__engine = databaseEngine

        # Reflect the tables from the database that we want to add data too
        metadata = MetaData()
        metadata.reflect(bind=databaseEngine)
        self.ref_dataLocation = metadata.tables['ref_dataLocation']
        self.ref_dataSource = metadata.tables['ref_dataSource']

        # Delete the data according to csv files
        self.remove_ref_dataLocation(self.readInitCSV('magnoliaLocations.csv'))
        self.remove_ref_dataSource(self.readInitCSV('semaphoreDataSource.csv'))

        return True
    
    def remove_ref_dataLocation(self, conditions: list[dict]) -> list[tuple]:
        """This method removes reference rows
            :param conditions: A list of dictionaries. The dict can be found in NOTE 1
            :return Series - SQLALCHEMY tupleish rows that were removed
            NOTE:: {"code": None, "displayName": None, "notes": None, "latitude": None, "longitude": None}
        """
        with self.__engine.connect() as conn:
            # First, fetch the rows that are going to be deleted (for returning)
            results = []
            for condition in conditions:
                stmt = select(self.ref_dataLocation).where(
                    *[getattr(self.ref_dataLocation.c, key) == value for key, value in condition.items()])
                results.extend(conn.execute(stmt).fetchall())

            # Now delete the rows
            for condition in conditions:
                delete_stmt = delete(self.ref_dataLocation).where(
                    *[getattr(self.ref_dataLocation.c, key) == value for key, value in condition.items()])
                conn.execute(delete_stmt)
                
            conn.commit()

        return results
        
    def remove_ref_dataSource(self, conditions: list[dict]) -> list[tuple]:
        """This method removes reference rows
            :param conditions: A list of dictionaries. The dict can be found in NOTE 1
            :return Series - SQLALCHEMY tupleish rows that were removed
            NOTE:: {"code": None, "displayName": None, "notes": None}
        """
        
        with self.__engine.connect() as conn:
            # First, fetch the rows that are going to be deleted (for returning)
            results = []
            for condition in conditions:
                stmt = select(self.ref_dataSource).where(
                    *[getattr(self.ref_dataSource.c, key) == value for key, value in condition.items()])
                results.extend(conn.execute(stmt).fetchall())

            # Now delete the rows
            for condition in conditions:
                delete_stmt = delete(self.ref_dataSource).where(
                    *[getattr(self.ref_dataSource.c, key) == value for key, value in condition.items()])
                conn.execute(delete_stmt)

            conn.commit()

        
        return results
        