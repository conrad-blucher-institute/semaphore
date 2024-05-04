# -*- coding: utf-8 -*-
#1.0DatabaseMigration.py
#----------------------------------
# Created By: Savannah Stephenson
# Created Date: 05/04/2024
# Version 1.0
#----------------------------------
"""This is a database migration script that will initialize version
    2.1 of the database. The change from version 2.0 to 2.1 is that
    we are __________________. 
 """ 
#----------------------------------
# 
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from sqlalchemy import Engine


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 2.1 which ___________
           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        # Update Method Here
        
        return True


    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database back to version 2.0 which involves
           removing the changes associated with version 2.1 
           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        # Rollback Method Here

        return True
        