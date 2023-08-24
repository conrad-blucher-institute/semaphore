# -*- coding: utf-8 -*-
#ISeriesStorage.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 8/20/2023
# version 2.0
#----------------------------------
"""This is an interface for Presistant Storage
 """ 
#----------------------------------
# 
#
#Imports
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from SeriesProvider.DataClasses import Series
from utility import log

from abc import ABC, abstractmethod
class ISeriesStorage(ABC):

    @abstractmethod
    def select_actuals(self, seriesDescription) -> Series:
        raise NotImplementedError()
    
    @abstractmethod
    def select_prediction(self, seriesDescription) -> Series:
        raise NotImplementedError()
    
    #TODO::Needs to be added
    # @abstractmethod
    # def select_output(self, seriesDescription) -> Series:
    #     raise NotImplementedError()
    
    @abstractmethod
    def find_external_location_code(self, sourceCode, location, priorityOrder: int = 0) -> str:
        raise NotImplementedError()
    

    @abstractmethod
    def insert_actuals(self, Series) -> Series:
        raise NotImplementedError()
    
    @abstractmethod
    def insert_predictions(self, Series) -> Series:
        raise NotImplementedError()
    
    @abstractmethod
    def insert_output(self, Series) -> Series:
        raise NotImplementedError()


