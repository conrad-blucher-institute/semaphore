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
from sqlalchemy import Engine


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 2.0 which adds the
           deployed_database_version table. 
           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        pass

    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database back to version 1.0 which involves
           removing the changes associated with verison 2.0 (the database version table)
           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        pass