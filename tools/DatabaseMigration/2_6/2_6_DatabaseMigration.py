# -*- coding: utf-8 -*-
#2_6_DatabaseMigration.py
#----------------------------------
# Created By: Savannah Stephenson
# Created Date: 09/19/2024
# Version 1.0
#----------------------------------
"""This is a database migration script that will update to version
    2.6 of the database. The intended change is adding user accounts
    for the API, Semaphore-Core, and Semaphore team members. 
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
CSV_FILE_PATHS = './tools/DatabaseMigration/2_6/init_data'


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 2.6 which adds user accounts.

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
            of the database.

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
    

    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database from 2.6.

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
