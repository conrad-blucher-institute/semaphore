# -*- coding: utf-8 -*-
#IDatabaseMigration.py
#----------------------------------
# Created By: Savannah Stephenson
# Created Date: 04/15/2024
# Version 1.0
#----------------------------------
"""This is an interface for Database Migration Methods. 
   The update method should be implemented to raise the version of a database, adding new rows or tables. 
   The rollback method should be implemented to lower the version of a database, removing changes.
   NOTE: Naming conventions of each version are as follows: any changes in schema goes up by a whole number (ex: 1.o -> 2.0),
                                                            any minor changes like adding a row a user, goes up by a minor number (ex. 2.3 -> 2.4)
 """ 
#----------------------------------
# 
#
#Imports
from abc import ABC, abstractmethod
from sqlalchemy import Engine

class IDatabaseMigration(ABC):

    @abstractmethod
    def update(self, databaseEngine: Engine) -> bool:
        raise NotImplementedError()
    
    @abstractmethod
    def rollback(self, databaseEngine: Engine) -> bool:
        raise NotImplementedError()