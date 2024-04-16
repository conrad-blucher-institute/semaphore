# -*- coding: utf-8 -*-
#IDatabaseMigration.py
#----------------------------------
# Created By: Savannah Stephenson
# Created Date: 04/15/2024
# Version 1.0
#----------------------------------
"""This is an interface for Database Migration Methods
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