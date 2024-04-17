# -*- coding: utf-8 -*-
#1.0DatabaseMigration.py
#----------------------------------
# Created By: Savannah Stephenson
# Created Date: 04/16/2024
# Version 1.0
#----------------------------------
"""This is a database migration script that will initialize version
    1.0 of the database (without using the ORM)
 """ 
#----------------------------------
# 
#
#Imports
from IDatabaseMigration import IDatabaseMigration

from sqlalchemy import Engine

#Implementing methods of interface
class Migration_1_0(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        #We are passed a database engine, so connect to the database
        #
        with databaseEngine.connect() as connection:
            pass

    def rollback(self, databaseEngine: Engine) -> bool:
        #We don't need a rollback method for this version 
        # so what should we do in here?
        pass

