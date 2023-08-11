# -*- coding: utf-8 -*-
#SeriesProvider.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/30/2023
# version 2.0
#----------------------------------
"""This class holds is the startpoint for interacting with the data section of semaphore. All data requests should go through here.
 """ 
#----------------------------------
# 
#
#Imports
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from SeriesStorage.SeriesStorage import SeriesStorage
from DataIngestion.DataIngestionMap import DataIngestionMap
from SeriesProvider.DataClasses import Series, LocalSeriesDescription, SeriesDescription, Actual, Prediction
from utility import log
from traceback import format_exc

from datetime import datetime
from typing import List, Dict


class DataManager():

    def __init__(self) -> None:
        self.seriesStorage = SeriesStorage()

    def get_SeriesStorage(self) -> SeriesStorage:
        """Get the seriesStorage this dataManager is using for debugging perposes."""
        return self.seriesStorage
    
    def make_SaveRequest(self, series: Series) -> Series:
        """Passes a series to the DBManager to be saved.
        Paremeters:
            series: series - The series request object detailing the request
        Returns:
            series: A response object returning what happened 
        """
        returningSeries = Series()
        returningSeries.description = series.description

        if not (type(series.description) == LocalSeriesDescription): #Check and make sure this is actually something with the proper description to be inserted
            self.__get_and_log_err_series([], returningSeries, f'A save Request must be provided a series with a LocalSeriesDescription \n')
        else:
            self.seriesStorage.s_prediction_output_insert(series)
            returningSeries.isComplete = True

        return returningSeries

    def make_request(self, requestDesc: SeriesDescription) -> Series:
        """This method attempts to fill a data request either by gettting the data from the DataBase or from DataIngestion. Will always return a series response.
        ------
        Parameters:
            requestDesc:SeriesDescription - A SeriesDescription obj detailing the request (src/DataManagement/DataClasses.py)
        Returns:
            Series - A series object holding either the requested data or a error message with the incomplete data. (src/DataManagement/DataClasses.py)
        """
        
        try:

            isPrediction, interval, seriesCode = self.__parse_series(requestDesc.series)
            
            ###Attempt to pull request from DB
            checkedResults = []
            dbResults = []
            if isPrediction:
                dbResults = self.seriesStorage.s_prediction_selection(requestDesc.source, requestDesc.series, requestDesc.location, requestDesc.fromDateTime, requestDesc.toDateTime, requestDesc.datum)
            else:
                dbResults = self.seriesStorage.s_data_point_selection(requestDesc.source, requestDesc.series, requestDesc.location, requestDesc.fromDateTime, requestDesc.toDateTime, requestDesc.datum)
            

            ###Check contents contains the right amount of results

            #Calculate the amnt of expected results
            amntExpected = self.__get_amnt_of_results_expected(interval, requestDesc.toDateTime, requestDesc.fromDateTime)
            if amntExpected is None:
                return self.__get_and_log_err_response(requestDesc, dbResults, f'Could not process series, {requestDesc.series}, interval value to determin amnt of expected results in request.')

            #First AmountCheck
            if len(dbResults) != amntExpected:

                #Call Data Ingestion to fetch data
                dataIngestionMap = DataIngestionMap(self.seriesStorage)
                diResults = dataIngestionMap.map_fetch(requestDesc)
                if diResults is None:
                    return self.__get_and_log_err_response(requestDesc, dbResults, f'DB did not have data request, dataIngestion returned NONE, for request.')
                
                #Merge data
                mergedResults = self.__merge_results(dbResults, diResults)

                #Second AmountCheck
                if(len(mergedResults) != amntExpected):
                    return self.__get_and_log_err_response(requestDesc, mergedResults, f'Merged Data Base Results and Data Ingestion Results failed to have the correct amount of results for request. Got:{len(mergedResults)} Expected:{amntExpected}')
                else:
                    checkedResults = mergedResults
            else:
                checkedResults = dbResults

            ###Generate proper response object
            responseSeries = Series()
            
            #Splice data down into data objects 
            #TODO:: MOve to data ingestion????
            try:
                if isPrediction:
                    responseSeries.bind_data(self.__splice_prediction_results(checkedResults))
                else:
                    responseSeries.bind_data(self.__splice_dataPoint_results(checkedResults))
            except Exception as e:
                return self.__get_and_log_err_response(requestDesc, mergedResults, f'An issue occured when attempting to splice returned data into dataObjs for request.\nException: {format_exc()}')
            
            return responseSeries
        
        except Exception as e:
            return self.__get_and_log_err_response(requestDesc, [], f'An unknown error occured attempting to fill request.\nRequest: {requestDesc}\nException: {format_exc()}')
    

    def __get_and_log_err_response(self, description: LocalSeriesDescription | SeriesDescription, currentData: List, msg: str) -> Series:
        """This function logs an error message as well as generating a Response object with the same message
        -------
        Parameters:
            description: LocalSeriesDescription | SeriesDescription - The description to pass back, either the output or input description
            currentData: List - Any data we already have, even if its not complete
            msg: str - The error message
        Returns:
            Series: A series object holding the error information and marked not complete.
        """
        log(msg)
        response = Series()
        response.isComplete = False
        response.nonCompleteReason = msg
        response.description = description
        response.bind_data(currentData)
        return response
    

    def __parse_series(self, series: str) -> tuple:
        """Parses a series object into its usable parts
        -------
        Parameters:
            series: str - The series to parse.
        Returns: (tuple)
            bool - If it is a prediction (true) or data point (false).
            str - A three char code indicating the interval the data is in.
            str = A six char unique code of the series.
        """
        isPrediction = (True if series[0] == 'p' else False)
        interval = series[1:4]
        seriesCode = series[4:]
        return isPrediction, interval, seriesCode
    
    
    def __get_amnt_of_results_expected(self, interval: str, toDateTime: datetime, fromDateTime: datetime) -> int | None:
        """Calculates the amount of records we should expect given a time span and an interval code
        -------
        Parameters:
            interval: str - A 3 char interval code.
            toDateTime: datetime - The late datetime.
            fromDateTime: datetime - The early datetime.
        Returns:
            int | None - Returns the amount of results or none if it can't map the interval to a method
        """
        totalSecondsrequested = (toDateTime - fromDateTime).total_seconds()
        amntOfResultsExpected = None
        
        match interval:
            case '1hr':
                secondsInInterval = 3600
            case '6mn':
                secondsInInterval = 360
            case _:
                return None
        
        if totalSecondsrequested < secondsInInterval: return 1 #Only one point was requested
        else: return int(totalSecondsrequested / secondsInInterval)
    

    def __merge_results(self, first: List[tuple], second: List[tuple]) -> List[Dict]:
        """Merges two lists of dictionaries together, will only keep unique results.
        -------
        Parameters:
            first List[Dict] - The first list to combine.
            second List[Dict] - The second list to combine.
        Returns:
            List[Dict] - The combined, unique List.
        """
        uniqueToSecond = set(second) - set(first)
        return first + list(uniqueToSecond)
    