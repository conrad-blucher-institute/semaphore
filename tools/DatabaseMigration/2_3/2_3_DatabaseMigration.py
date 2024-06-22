# -*- coding: utf-8 -*-
#2_1_DatabaseMigration.py
#----------------------------------
# Created By: Anointiyae Beasley
# Created Date: 06/18/2024
# Version 1.0
#----------------------------------
"""This is a database migration script that will initialize version
    2.3 of the database. The change from version 2.2 to 2.3 is that
    we are adding one row to the datum reference table.
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
CSV_FILE_PATHS = './tools/DatabaseMigration/2_3/init_data'

class Migrator(IDatabaseMigration):
    
    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 2.3. One row will be added to the datum reference table.
           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        # Setting engine
        self.__engine = databaseEngine
        
        # Reflect the tables from the database that we want to add data to
        metadata = MetaData()
        metadata.reflect(bind=databaseEngine)
        
        ref_dataDatum = metadata.tables['ref_dataDatum']
        
         # Insert the data based off of csv files
        self.insert_ref_data(self.readInitCSV('magnoliaDatum.csv'), ref_dataDatum)
        
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
        """This function rolls the database back to version 2.2 which involves removing the changes 
           associated with version 2.3 

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        fileNames = ['magnoliaDatum.csv']
        fileTypes = [ KeywordType.DATA_DATUM]

        # Using the utility helper class to delete any data dependent on the rows added in the 2.3 Migration
        helper = DatabaseDeletionHelper(databaseEngine)

        for file, type in zip(fileNames, fileTypes):
            for rowDict in self.readInitCSV(file):
                helper.deep_delete_keyword(rowDict["code"], type)

        return True