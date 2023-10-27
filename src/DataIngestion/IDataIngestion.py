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
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from DataClasses import SeriesDescription, Series, TimeDescription
from utility import log

from abc import ABC, abstractmethod
from importlib import import_module



class IDataIngestion(ABC):

    @abstractmethod
    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        raise NotImplementedError
    

def data_ingestion_factory(request: SeriesDescription) -> IDataIngestion:
    """Uses the source atribute of a data request to dynamically import a module
    ------
    Parameters
        request: SeriesDescription - A data SeriesDescription object with the information to pull (src/DataManagment/DataClasses>Series)
    Returns
        IDataIngestion - An child of the IDataIngestion interface.
    """
    try:
        return getattr(import_module(f'src.DataIngestion.DataIngestion.{request.source}'), f'{request.source}')()
    except Exception:
        raise ModuleNotFoundError(f'No module named {request.source} in src.DataIngestion.DataIngestion!')
    