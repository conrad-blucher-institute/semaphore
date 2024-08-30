# -*- coding: utf-8 -*-
#2_4_DatabaseMigration.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date:8/28/2024
# Version 1.0
#----------------------------------
"""This is a database migration script that will update to version
    2.4 of the database. The intended change is fixing some incorrect meta data. 
 """ 
#----------------------------------
# 
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from DatabaseMigration.databaseMigrationUtility import KeywordType, DatabaseDeletionHelper
from sqlalchemy import Engine, MetaData, Table, delete, select, update
from sqlalchemy.dialects.postgresql import insert 
import csv

#Constants
CSV_FILE_PATHS = './tools/DatabaseMigration/2_4/init_data'


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 2.4 which update Series descriptions

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
        old_data = self.__readInitCSV('oldRows.csv')

        results = self.__update_ref_data(old_data, new_data, ref_dataSeries)

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
    
    def __update_ref_data(self, old: list[dict], new: list[dict], table: Table) -> list[tuple]:
        """This method inserts reference rows
            :param old: the old rows to update.
            :param new: The new rows to update.
            :return Series - SQLALCHEMY tupleish rows
            NOTE:: {"code": None, "displayName": None, "notes": None}
            NOTE:: Old and new rows MUST be in the same order!
        """

        results = []
        with self.__engine.connect() as conn:
            for old, new in zip(old, new):
                cursor = conn.execute(update(table)
                                    .where(table.c.code == old['code'])
                                    .where(table.c.displayName == old['displayName'])
                                    .where(table.c.notes == old['notes'])
                                    .returning(table)
                                    .values(new)
                                    )
                results.append(cursor.fetchall())
            conn.commit() # Commit only after every update is successful
        return results

    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database from 2.4

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        # Setting engine
        self.__engine = databaseEngine

        # Reflect the tables from the database that we want to add data too
        metadata = MetaData()
        metadata.reflect(bind=databaseEngine)
        
        ref_dataSeries = metadata.tables['ref_dataSeries']

        # Read in the data based off of csv files, but we swap old to new and new to old as this is a rollback
        old_data = self.__readInitCSV('newRows.csv')
        new_data = self.__readInitCSV('oldRows.csv')

        results = self.__update_ref_data(old_data, new_data, ref_dataSeries)

        return True if len(results) == 3 else False
