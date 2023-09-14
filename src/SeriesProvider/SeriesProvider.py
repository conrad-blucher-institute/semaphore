# -*- coding: utf-8 -*-
#SeriesProvider.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/30/2023
# version 2.0
#----------------------------------
"""This class holds is the start point for interacting with the data section of semaphore. All data requests should go through here.
 """ 
#----------------------------------
# 
#
#Imports
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from SeriesStorage.SeriesStorage.SS_Map import map_to_SS_Instance
from DataIngestion.IDataIngestion import map_to_DI_Instance
from DataClasses import Series, SemaphoreSeriesDescription, SeriesDescription, Actual, Prediction, Output
from utility import log
from traceback import format_exc

from datetime import datetime
from typing import List, Dict


class SeriesProvider():

    def __init__(self) -> None:
        self.seriesStorage = map_to_SS_Instance()
    
    def make_SaveRequest(self, series: Series) -> Series:
        """Passes a series to the DBManager to be saved.
        Parameters:
            series: series - The series request object detailing the request
        Returns:
            series: A response object returning what happened 
        """
        returningSeries = Series(series.description, True)

        if not (type(series.description) == SemaphoreSeriesDescription): #Check and make sure this is actually something with the proper description to be inserted
            self.__get_and_log_err_series([], returningSeries, f'A save Request must be provided a series with a SemaphoreSeriesDescription \n')
        else:
            self.seriesStorage.insert_output(series)
            returningSeries.isComplete = True

        return returningSeries

    def make_request(self, requestDesc: SeriesDescription) -> Series:
        """This method attempts to fill a data request either by getting the data from the DataBase or from DataIngestion. Will always return a series response.
        ------
        Parameters:
            requestDesc:SeriesDescription - A SeriesDescription obj detailing the request (src/DataManagement/DataClasses.py)
        Returns:
            Series - A series object holding either the requested data or a error message with the incomplete data. (src/DataManagement/DataClasses.py)
        """
        
        try:
            
            ###Attempt to pull request from DB

            #Checking which database table to pull data from
            match requestDesc.dataClassification:
                case 'actual': 
                    dbSeries = self.seriesStorage.select_actuals(requestDesc)
                case 'prediction':
                    dbSeries = self.seriesStorage.select_prediction(requestDesc)
                case _:
                    log(f'Data Classification {requestDesc.dataClassification} was unable to be matched by series provider.')
                    raise NotImplementedError
                    
            dbData = dbSeries.data
            

            ###Check contents contains the right amount of results

            #Calculate the amnt of expected results
            amntExpected = self.__get_amnt_of_results_expected(requestDesc)
            if amntExpected is None:
                return self.__get_and_log_err_response(requestDesc, dbSeries, f'Could not process series, {requestDesc.series}, interval value to determin amnt of expected results in request.')

            #First AmountCheck
            if len(dbData) != amntExpected:

                #Call Data Ingestion to fetch data
                diClass = map_to_DI_Instance(requestDesc)
                diResults = diClass.ingest_series(requestDesc)

                if diResults is None:
                    return self.__get_and_log_err_response(requestDesc, dbSeries, f'DB did not have data request, dataIngestion returned NONE, for request.')
                diData = diResults.data
                
                #Merge data
                mergedResults = self.__merge_results(dbData, diData)
                #Second AmountCheck
                if(len(mergedResults) != amntExpected):
                    print(mergedResults)
                    return self.__get_and_log_err_response(requestDesc, mergedResults, f'Merged Data Base Results and Data Ingestion Results failed to have the correct amount of results for request. Got:{len(mergedResults)} Expected:{amntExpected}')
                else:
                    checkedResults = mergedResults
            else:
                checkedResults = dbData

            ###Generate proper response object
            responseSeries = Series(requestDesc, True)
            responseSeries.data = checkedResults
            return responseSeries
        
        except Exception as e:
            return self.__get_and_log_err_response(requestDesc, [], f'An unknown error occurred attempting to fill request.\nRequest: {requestDesc}\nException: {format_exc()}')
    
    def make_output_request(self, requestDesc: SemaphoreSeriesDescription) -> Series: 
        ''' Takes a description of an output series and attempts to return it
        :param requestDesc: SemaphoreSeriesDescription -A semaphore series description
        :return series
        '''
        ###See if we can get the outputs from the database
        dbi = map_to_SS_Instance() 
        requestedSeries = dbi.select_output(requestDesc)

        ###Do we have enough outputs? 
        expected = self.__get_amnt_of_results_expected(requestDesc)
        if (len(requestedSeries.data) == expected): 
            requestedSeries.isComplete = True
        else: 
            requestedSeries.isComplete = False
            error = f"This description {requestDesc} had incomplete data."
            requestedSeries.nonCompleteReason = error
            log(error) 

        ###return that series object to the requester'
        return requestedSeries
        

    def __get_and_log_err_response(self, description: SemaphoreSeriesDescription | SeriesDescription, currentData: List, msg: str) -> Series:
        """This function logs an error message as well as generating a Response object with the same message
        -------
        Parameters:
            description: SemaphoreSeriesDescription | SeriesDescription - The description to pass back, either the output or input description
            currentData: List - Any data we already have, even if its not complete
            msg: str - The error message
        Returns:
            Series: A series object holding the error information and marked not complete.
        """
        log(msg)
        response = Series(description, False, msg)
        response.data = currentData
        return response
    
    
    def __get_amnt_of_results_expected(self, seriesDescription: SeriesDescription | SemaphoreSeriesDescription) -> int:
        """Calculates the amount of records we should expect given a time span and an interval code
        -------
        Parameters:
            seriesDescription: SeriesDescription
        Returns:
            int - Returns the amount of results
        """
        totalSecondsRequested = (seriesDescription.toDateTime - seriesDescription.fromDateTime).total_seconds()
    
        if totalSecondsRequested < seriesDescription.interval: return 1 #Only one point was requested
        else: return int(totalSecondsRequested / seriesDescription.interval) + 1
    

    def __merge_results(self, first: List[Actual | Prediction], second: List[Actual | Prediction]) -> List[Dict]:
        """Merges two lists of dictionaries together, will only keep unique results.
        -------
        Parameters:
            first List[Actual | Prediction] - The first list to combine.
            second List[Actual | Prediction] - The second list to combine.
        Returns:
            List[Dict] - The combined, unique List.
        """

        #TODO:: This is a very slow, it was optimized with hashing like this:    
            # uniqueToSecond = set(second) - set(first)
            # return first + list(uniqueToSecond)
        #But its not hashing properly anymore, needs to be looked into

        for actual in second:
            if not actual in first: first.append(actual)

        return first
    