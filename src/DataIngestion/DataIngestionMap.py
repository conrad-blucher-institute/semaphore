# -*- coding: utf-8 -*-
#DataIngestionMap.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/15/2023
# version 1.0
#----------------------------------
"""This file holds the DataIngestionMap class. It maps a data fetch request to the propper class and method. Preforms the call and returns the result. 
It includes some methods for debugging perposes.
 """ 
#----------------------------------
# 
#
#Input
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from PersistentStorage.DBManager import DBManager
from DataManagement.DataClasses import Request
from utility import log

from typing import List, Dict

class DataIngestionMap():

    def __init__(self, dbManager: DBManager) -> None:
        self.dbManager = dbManager

    def get_dbManager(self):
        """Returns the dbManager used by this dataIngestionMap for debugging perposes"""
        return self.dbManager

    def map_fetch(self, request: Request) -> List[Dict] | None:
        """Maps a data fetch request to the propper class and method. Preforms the call and returns the result.
        ------
        Parameters
            request: Request - A data Request object with the information to pull (src/DataManagment/DataClasses>Request)
        Returns
            List[Dict] | None - Either a list of the succesffuly inserted rows in the db or None if something went wrong
        """

        match request.source:
            case 'noaaT&C':
                return self.__noaaTandC(request)
            case _:
                log(f'Data source: {request.source}, not found in data ingestion map for request: {request}!')
                return False
            
    def __noaaTandC(self, request: Request) -> List[Dict] | None:
        """Maps noaaTandC fetch request to proper function. Preforms the call and returns the result.
        ------
        Parameters
            request: Request - A data Request object with the information to pull (src/DataManagment/DataClasses>Request)
        Returns
            List[Dict] | None - Either a list of the succesffuly inserted rows in the db or None if something went wrong
        """
        from NOAATidesAndCurrents import NOAATidesAndCurrents
        noaa = NOAATidesAndCurrents(self.dbManager)

        match request.series:
            case 'WlHr':
                return noaa.fetch_water_level_hourly(request)
            case _:
                log(f'Data series: {request.series}, not found for NOAAT&C for request: {request}')
                return False

    
    