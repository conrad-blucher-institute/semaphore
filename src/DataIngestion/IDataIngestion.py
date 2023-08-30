# -*- coding: utf-8 -*-
#OutputManager.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 8/20/2023
# version 1.0
#----------------------------------
"""This is an interface for Data
Methods.
 """ 
#----------------------------------
# 
#
#Imports
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from SeriesProvider.DataClasses import SeriesDescription, Series
from utility import log

from abc import ABC, abstractmethod



class IDataIngestion(ABC):

    @abstractmethod
    def ingest_series(self, seriesDescription: SeriesDescription) -> Series | None:
        raise NotImplementedError
    

def map_to_DI_Instance(request: SeriesDescription) -> IDataIngestion:
    """Maps a series description request to the proper class and method. Preforms the call and returns the result.
    ------
    Parameters
        request: SeriesDescription - A data SeriesDescription object with the information to pull (src/DataManagment/DataClasses>Series)
    Returns
        series | None - Either the series returned by the given function or None
    """

    match request.source:
        case 'noaaT&C':
            from DataIngestion.DataIngestion.DI_NOAATidesAndCurrents import NOAATidesAndCurrents
            return NOAATidesAndCurrents()
        case _:
            log(f'Data source: {request.source}, not found in data ingestion map for request: {request}!')
            return None