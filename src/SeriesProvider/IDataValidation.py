# -*- coding: utf-8 -*-
#IDataValidation.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 9/23/2023
# version 1.0
#----------------------------------
"""This is an interface for ways of validating if a series is complete or not
 """ 
#----------------------------------
# 
#
#Imports
from DataClasses import SeriesDescription, Series

from abc import ABC, abstractmethod
from importlib import import_module



class IDataValidation(ABC):

    @abstractmethod
    def validate_series(self, series: Series) -> bool:
        raise NotImplementedError
    

def data_validation_factory(seriesRequest: SeriesDescription) -> IDataValidation:
    """Uses the source attribute of a data request to dynamically import a module
        :param seriesRequest: SeriesDescription - A data SeriesDescription object with the information to pull (src/DataManagment/DataClasses>SeriesDescription)
    """
    try:
        return getattr(import_module(f'.DV_Classes.{seriesRequest.verificationMethod}', 'DataValidation'), f'{seriesRequest.verificationMethod}')()
    except ModuleNotFoundError:
        raise ModuleNotFoundError(f'No module named {seriesRequest.verificationMethod} in DV_Classes!')
    