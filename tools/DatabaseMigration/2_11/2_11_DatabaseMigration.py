# -*- coding: utf-8 -*-
#2_11_DatabaseMigration.py
#----------------------------------
# Created By: Jeremiah Sosa
# Created Date: 01/03/2025
# Version 1.0
#----------------------------------
"""This is a database migration script that will initialize version
    2.11 of the database. The change from version 2.10 to 2.11 is that 
    were adding a reference table to map the error codes.
 """ 
#----------------------------------
# 
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from sqlalchemy import Table, Column, Integer, String, MetaData, Engine
from sqlalchemy.dialects.postgresql import insert
import csv

#Constants
CSV_FILE_PATHS = './tools/DatabaseMigration/2_11/init_data'


class Migrator(IDatabaseMigration):

    def readInitCSV(self, csvFileName: str) -> list:
        """This function reads in a CSV file
            :param csvFileName: str - CSV file name
            
            :return: Semaphore error codes
        """
        csvFilePath = f'{CSV_FILE_PATHS}/{csvFileName}'
        errorCodes = []
        with open(csvFilePath, mode = 'r') as infile:
            csvDict = csv.DictReader(infile)
            for row in csvDict:
                errorCodes.append({
                    "code": int(row['code']),
                    "result": row['result']
                })

        return errorCodes

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 2.11 which adds the
           ref_predictionResults table. 
           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        #create the schema of the additional table 
        self.__create_schema()

        #first row with error codes
        error_codes = self.readInitCSV('errorCodes.csv')

        #starting a transaction
        with databaseEngine.begin() as connection:
            #add new table to the database
            self._metadata.create_all(connection)
            #insert the first row error codes
            connection.execute(insert(self.ref_predictionResults)
                               .values(error_codes))
            connection.commit()

        return True

    def __create_schema(self) -> None:
        """Builds the db schema for the version table in the metadata.
        """

        self._metadata = MetaData()

        #this table stores the current version of the database
        self.ref_predictionResults = Table(
            "ref_predictionResults",
            self._metadata,

            Column("code", Integer, primary_key=True),
            Column("result", String(32), nullable=False)
        )


    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database back to version 2.10 which involves
           removing the changes associated with version 2.11.
           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """

        self.__create_schema()
        #starting a transaction
        with databaseEngine.begin() as connection:
            #dropping the table
            self.ref_predictionResults.drop(connection)

        return True