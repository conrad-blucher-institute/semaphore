# -*- coding: utf-8 -*-
#1.0DatabaseMigration.py
#----------------------------------
# Created By: Savannah Stephenson
# Created Date: 04/24/2024
# Version 1.0
#----------------------------------
"""This is a database migration script that will initialize version
    2.0 of the database. The change from version 1.0 to 2.0 is that
    we are adding a table to the database tht holds the current
    database version. 
 """ 
#----------------------------------
# 
#
#Imports
from IDatabaseMigration import IDatabaseMigration
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, Engine
from sqlalchemy.dialects.postgresql import insert
import datetime


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 2.0 which adds the
           deployed_database_version table. 
           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        #create the schema of the additional table 
        self.__create_schema()

        #build dictionary with first row information
        first_row_dict = [{'versionNumber':'2.0', 'migrationTime':datetime.utcnow(), 'versionNotes': " "}]
        
        #starting a transaction
        with databaseEngine.begin() as connection:
            #add new table to the database
            self._metadata.create_all(connection)
            #insert the first row
            connection.execute(insert(self.deployed_database_version)
                               .value(first_row_dict))
            connection.commit()
        
        return True

    def __create_schema(self) -> None:
        """Builds the db schema for the version table in the metadata.
        """

        self._metadata = MetaData()
        
        #this table stores the current version of the database
        self.deployed_database_version = Table(
            "deployed_database_version",
            self._metadata,

            Column("id", Integer, autoincrement=True, primary_key=True),

            Column("versionNumber", String(10), nullable=False),

            Column("migrationTime", DateTime, nullable=False),

            Column("versionNotes", String(255), nullable=False)
        )
        

    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database back to version 1.0 which involves
           removing the changes associated with verison 2.0 (the database version table)
           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """

        #setting engine
        self.__engine = databaseEngine

         #starting a transaction
        with databaseEngine.begin() as connection:
            #dropping the table
            self.deployed_database_version.drop(connection)

        return True
        