# -*- coding: utf-8 -*-
#2_5_DatabaseMigration.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date:9/19/2024
# Version 1.0
#----------------------------------
"""This is a database migration script that will update to version
    2.5 of the database. The intended change is add series for NDBC
 """ 
#----------------------------------
# 
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from DatabaseMigration.databaseMigrationUtility import KeywordType, DatabaseDeletionHelper
from sqlalchemy import Engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert 
import csv

#Constants
CSV_FILE_PATHS = './tools/DatabaseMigration/2_5/init_data'


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 2.5 which update Series descriptions

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        # Setting engine
        self.__engine = databaseEngine

        # Reflect the tables from the database that we want to add data too
        metadata = MetaData()
        metadata.reflect(bind=databaseEngine)
        
        ref_dataSeries = metadata.tables['ref_dataSeries']

        # Read in the data based off of csv files
        new_data = self.__readInitCSV('newRows.csv')
        results = self.insert_ref_data(new_data, ref_dataSeries)

        return True if len(results) == 3 else False
    
    def __readInitCSV(self, csvFileName: str) -> list:
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
        """This function rolls the database back to version 2.6 which involves removing the changes 
           associated with version 2.5.

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        # Note that we only explicitly include the locations csv here
        # because a part of the deep_delete protocol ofr the data locations
        # table is to delete the location from the mapping table. 
        fileNames = ['newRows.csv']
        fileTypes = [KeywordType.DATA_SERIES]

        # Using the utility helper class to delete any data dependent on the rows added in the 2.1 Migration
        helper = DatabaseDeletionHelper(databaseEngine)

        for file, type in zip(fileNames, fileTypes):
            for rowDict in self.__readInitCSV(file):
                helper.deep_delete_keyword(rowDict["code"], type)

        return True