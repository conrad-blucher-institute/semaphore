# -*- coding: utf-8 -*-
#2_10_DatabaseMigration.py
#----------------------------------
# Created By: Jeremiah Sosa
# Created Date: 2/25/2025
# Version 1.0
#----------------------------------
"""This is a database migration script that will initialize version
    2.12 of the database. The change from version 2.11 to 2.12 will 
    remove the artifact in the dataSourceLocationCode column and replaces it 
    with a 0.
 """ 
#----------------------------------
# 
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from DatabaseMigration.databaseMigrationUtility import DatabaseDeletionHelper
from sqlalchemy import MetaData, Engine, update


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 2.12.
           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """

        deletion_helper = DatabaseDeletionHelper(databaseEngine)

        deletion_helper.delete_mapping_row(
            dataLocationCode = 'lagunamadre',
            dataSourceCode = 'LIGHTHOUSE'
        )

        return True
        

    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database back to version 2.11 which involves
           removing the changes associated with version 2.12.
           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """

        # Reflect the tables from the database that we want to add data too
        metadata = MetaData()
        metadata.reflect(bind=databaseEngine)
        
        dataLocation_dataSource_mapping = metadata.tables['dataLocation_dataSource_mapping']
        
        #starting a transaction
        with databaseEngine.begin() as connection:

            connection.execute(
                dataLocation_dataSource_mapping.insert().values(
                    dataLocationCode = 'lagunamadre',
                    dataSourceCode = 'LIGHTHOUSE',
                    dataSourceLocationCode = '48646513645',
                    priorityOrder = 0
                )
            )

        return True
        