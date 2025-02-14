# -*- coding: utf-8 -*-
#2_10_DatabaseMigration.py
#----------------------------------
# Created By: Anointiyae Beasley
# Created Date: 1/22/2024
# Version 1.0
#----------------------------------
"""This is a database migration script that will initialize version
    2.12 of the database. The change from version 2.11 to 2.12 will 
    remove the ...
 """ 
#----------------------------------
# 
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from sqlalchemy import MetaData, Engine, update
from datetime import datetime


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 2.12.
           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """

        # Reflect the tables from the database that we want to add data too
        metadata = MetaData()
        metadata.reflect(bind=databaseEngine)
        
        ref_dataLocation = metadata.tables['ref_dataLocation']
        
        #starting a transaction
        with databaseEngine.begin() as connection:

            connection.execute(
                update(ref_dataLocation)
                .where(ref_dataLocation.c.code == 'lagunamadre')
                .values(latitude=26.75, longitude=-97.4167)
            )
        
        return True
        

    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database back to version 2.11 which involves
           removing the changes associated with version 2.12 (- - - -)
           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """

        # Reflect the tables from the database that we want to add data too
        metadata = MetaData()
        metadata.reflect(bind=databaseEngine)
        
        ref_dataLocation = metadata.tables['ref_dataLocation']
        
        #starting a transaction
        with databaseEngine.begin() as connection:

            connection.execute(
                update(ref_dataLocation)
                .where(ref_dataLocation.c.code == 'lagunamadre')
                .values(latitude=0, longitude=0)
            )

        return True
        