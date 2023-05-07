# -*- coding: utf-8 -*-
#DataManager.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/30/2023
# version 1.0
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

from PersistentStorage.DBManager import DBManager
from DataIngestion.DataIngestionMap import DataIngestionMap
from DataManagement.DataClasses import Request, Response, DataPoint, Prediction
from utility import log
from traceback import format_exc

from datetime import datetime
from typing import List, Dict


class DataManager():

    def __init__(self) -> None:
        self.dbManager = DBManager()

    def get_dbManager(self) -> DBManager:
        """Get the dbManager this dataManager is using for debugging perposes."""
        return self.dbManager

    def make_request(self, request: Request) -> Response:
        """This method attempts to fill a data request either by gettting the data from the DataBase or from DataIngestion. Will always return a response.
        ------
        Parameters:
            request: Request - A request object filled with the information needed to fulfill the request. (src/DataManagement/DataClasses.py)
        Returns:
            Response - A response object holding either the requested data or a error message. (src/DataManagement/DataClasses.py)
        """
        try:
            isPrediction, interval, seriesCode = self.__parse_series(request.series)
            
            ###Attempt to pull request from DB
            checkedResults = []
            dbResults = []
            if isPrediction:
                dbResults = self.dbManager.s_prediction_selection(request.source, request.series, request.location, request.fromDateTime, request.toDateTime, request.datum)
            else:
                dbResults = self.dbManager.s_data_point_selection(request.source, request.series, request.location, request.fromDateTime, request.toDateTime, request.datum)
            

            ###Check contents contains the right amount of results

            #Calculate the amnt of expected results
            amntExpected = self.__get_amnt_of_results_expected(interval, request.toDateTime, request.fromDateTime)
            if amntExpected is None:
                return self.__get_and_log_err_response(request, dbResults, f'Could not process series, {request.series}, interval value to determin amnt of expected results in request.')

            #First AmountCheck
            if len(dbResults) != amntExpected:

                #Call Data Ingestion to fetch data
                dataIngestionMap = DataIngestionMap(self.dbManager)
                diResults = dataIngestionMap.map_fetch(request)
                if diResults is None:
                    return self.__get_and_log_err_response(request, dbResults, f'DB did not have data request, dataIngestion returned NONE, for request.')
                
                #Merge data
                mergedResults = self.__merge_results(dbResults, diResults)

                #Second AmountCheck
                if(len(mergedResults) != amntExpected):
                    return self.__get_and_log_err_response(request, mergedResults, f'Merged Data Base Results and Data Ingestion Results failed to have the correct amount of results for request. Got:{len(mergedResults)} Expected:{amntExpected}')
                else:
                    checkedResults = mergedResults
            else:
                checkedResults = dbResults

            ###Generate proper response object
            response = Response(request, True)
            
            #Splice data down into data objects 
            try:
                if isPrediction:
                    response.data = self.__splice_prediction_results(checkedResults)
                else:
                    response.data = self.__splice_dataPoint_results(checkedResults)
            except Exception as e:
                return self.__get_and_log_err_response(request, mergedResults, f'An issue occured when attempting to splice returned data into dataObjs for request.\nException: {format_exc()}')
            
            return response
        
        except Exception as e:
            return self.__get_and_log_err_response(request, [], f'An unknown error occured attempting to fill request.\nException: {format_exc()}')
    

    def __get_and_log_err_response(self, currentData: List, request: Request, msg: str) -> Response:
        """This function logs an error message as well as generating a Response object with the same message
        -------
        Parameters:
            request: Request - The request that caused the error
            msg: str - The error message
        Returns:
            Response: A response object holding the error information.
        """
        log(msg)
        response = Response(request, False, msg)
        response.data = currentData
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
        
        amntOfResultsExpected = int(totalSecondsrequested / secondsInInterval) + 1
        return amntOfResultsExpected
    

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
    

    def __splice_prediction_results(self, results: List[tuple]) -> List[Prediction]:
        """Splices up a list of dbresults, pulling out only the data that changes per point,
        and places them in a Prediction object.
        -------
        Parameters:
            first List[tuple] - The collection of dbrows.
        Returns:
            List[Prediction] - The formatted objs.
        """
        valueIndex = 3
        unitIndex = 4
        leadTimeIndex = 2
        timeGeneratedIndex = 1
        resultCodeIndex = 5
        predictions = []
        for row in results:
            predictions.append(Prediction(
                row[valueIndex],
                row[unitIndex],
                row[leadTimeIndex],
                row[timeGeneratedIndex],
                row[resultCodeIndex]
            ))

        return predictions
        

    def __splice_dataPoint_results(self, results: List[tuple]) -> List[DataPoint]:
        """Splices up a list of dbresults, pulling out only the data that changes per point,
        and places them in a DataPoint object.
        -------
        Parameters:
            first List[tuple] - The collection of dbrows.
        Returns:
            List[Prediction] - The formatted objs.
        """
        valueIndex = 3
        unitIndex = 4
        timeActualizedIndex = 1
        dataPoints = []
        for row in results:
            dataPoints.append(DataPoint(
                row[valueIndex],
                row[unitIndex],
                row[timeActualizedIndex]
            ))

        return dataPoints