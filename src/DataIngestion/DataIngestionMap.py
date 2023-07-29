# -*- coding: utf-8 -*-
#DataIngestionMap.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/15/2023
# version 2.0
#----------------------------------
"""This file holds the DataIngestionMap class. This calss is supposed to make a series and sorce to a givin class
and method to ingest the data.
 """ 
#----------------------------------
# 
#
#Input
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from SeriesStorage.SeriesStorage import SeriesStorage
from SeriesProvider.DataClasses import Series, SeriesDescription
from utility import log

class DataIngestionMap():

    def __init__(self, seriesStorage: SeriesStorage) -> None:
        self.seriesStorage = seriesStorage

    def get_seriesStorage(self) -> SeriesStorage:
        """Returns the seriesStorage used by this dataIngestionMap for debugging perposes"""
        return self.seriesStorage

    def map_fetch(self, request: SeriesDescription) -> Series | None:
        """Maps a data fetch request to the propper class and method. Preforms the call and returns the result.
        ------
        Parameters
            request: SeriesDescription - A data SeriesDescription object with the information to pull (src/DataManagment/DataClasses>Series)
        Returns
            series | None - Either the seires returned by the given function or None
        """

        match request.source:
            case 'noaaT&C':
                return self.__noaaTandC(request)
            case _:
                log(f'Data source: {request.source}, not found in data ingestion map for request: {request}!')
                return None
            
    def __noaaTandC(self, request: SeriesDescription) -> Series | None:
        """Maps noaaTandC fetch request to proper function. Preforms the call and returns the result.
        ------
        Parameters
            request: SeriesDescription - A data SeriesDescription object with the information to pull (src/DataManagment/DataClasses>Series)
        Returns
            series | None - Either the seires returned by the given function or None
        """
        from DataIngestion.NOAATidesAndCurrents import NOAATidesAndCurrents
        noaa = NOAATidesAndCurrents(self.seriesStorage)

        match request.series:
            case 'd1hrWl':
                return noaa.fetch_water_level_hourly(request)
            case 'd1hrXWnCmp':
                return noaa.fetch_X_wind_componants_hourly(request)
            case 'd1hrYWnCmp':
                return noaa.fetch_Y_wind_componants_hourly(request)
            case 'd6mnXWnCmp':
                return noaa.fetch_X_wind_componants_6min(request)
            case 'd6mnYWnCmp':
                return noaa.fetch_Y_wind_componants_6min(request)
            case 'd1hrSurge':
                return noaa.fetch_surge_hourly(request)
            case _:
                log(f'Data series: {request.series}, not found for NOAAT&C for request: {request}')
                return None

    
    