# -*- coding: utf-8 -*-
#IDataIngestion.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 8/20/2023
# version 2.0
#----------------------------------
"""This is an interface for Data
Methods. As well as the factory to generate the instance of the interface
 """ 
#----------------------------------
# 
#
#Imports
from src.DataClasses import SeriesDescription, Series, TimeDescription

from abc import ABC, abstractmethod
from importlib import import_module



class IDataIngestion(ABC):

    @abstractmethod
    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        raise NotImplementedError
    

def data_ingestion_factory(seriesRequest: SeriesDescription) -> IDataIngestion:
    """Uses the source atribute of a data request to dynamically import a module
        :param seriesRequest: SeriesDescription - A data SeriesDescription object with the information to pull (src/DataManagment/DataClasses>SeriesDescription)
    """
    try:
        return getattr(import_module(f'.DI_Classes.{seriesRequest.dataSource}', 'DataIngestion'), f'{seriesRequest.dataSource}')()
    except ModuleNotFoundError:
        raise ModuleNotFoundError(f'No module named {seriesRequest.dataSource} in DI_Classes!')
    