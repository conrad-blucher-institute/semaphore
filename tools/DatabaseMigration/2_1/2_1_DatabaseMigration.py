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
from DatabaseMigration.databaseMigrationUtility import KeywordType, DatabaseDeletionHelper
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
        
        ref_dataLocation = metadata.tables['ref_dataLocation']
        ref_dataSource = metadata.tables['ref_dataSource']
        ref_dataLocation_dataSource_mapping = metadata.tables['dataLocation_dataSource_mapping']
        ref_dataDatum = metadata.tables['ref_dataDatum']
        ref_dataSeries = metadata.tables['ref_dataSeries']

        # Insert the data based off of csv files
        self.insert_ref_data(self.readInitCSV('magnoliaLocations.csv'), ref_dataLocation)
        self.insert_ref_data(self.readInitCSV('semaphoreDataSource.csv'), ref_dataSource)
        self.insert_ref_data(self.readInitCSV('magnoliaMapping.csv'), ref_dataLocation_dataSource_mapping)
        self.insert_ref_data(self.readInitCSV('magnoliaDatums.csv'), ref_dataDatum)
        self.insert_ref_data(self.readInitCSV('magnoliaSeries.csv'), ref_dataSeries)

        
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
    
    def insert_ref_data(self, rows: list[dict], table: Table) -> list[tuple]:
        """This method inserts reference rows
            :param rows: A list of dictionaries. The dict can be found in NOTE 1
            :return Series - SQLALCHEMY tupleish rows
            NOTE:: {"code": None, "displayName": None, "notes": None, "latitude": None, "longitude": None}
        """
        with self.__engine.connect() as conn:
            cursor = conn.execute(insert(table)
                                  .returning(table)
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
        fileNames = ['magnoliaLocations.csv', 'semaphoreDataSource.csv', 'magnoliaDatums.csv', 'magnoliaSeries.csv']
        fileTypes = [KeywordType.DATA_LOCATION, KeywordType.DATA_SOURCE, KeywordType.DATA_DATUM, KeywordType.DATA_SERIES]

        # Using the utility helper class to delete any data dependent on the rows added in the 2.1 Migration
        helper = DatabaseDeletionHelper(databaseEngine)

        for file, type in zip(fileNames, fileTypes):
            for rowDict in self.readInitCSV(file):
                helper.deep_delete_keyword(rowDict["code"], type)

        return True