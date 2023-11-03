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

from SeriesStorage.ISeriesStorage import series_storage_factory
from DataIngestion.IDataIngestion import data_ingestion_factory
from DataClasses import Series, SemaphoreSeriesDescription, SeriesDescription, TimeDescription



class SeriesProvider():

    def __init__(self) -> None:
        self.seriesStorage = series_storage_factory()
    
    def save_series(self, series: Series) -> Series:
        """Passes a series to Series Storage to be stored.
            :param series - The series to store.
            :returns series - A series containing only the stored values.
        """
        returningSeries = Series(series.description, True)

        if not (type(series.description) == SemaphoreSeriesDescription): #Check and make sure this is actually something with the proper description to be inserted
            returningSeries.isComplete = False
            returningSeries.nonCompleteReason = f'A save Request must be provided a series with a SemaphoreSeriesDescription not a Series Description'
        else:
            returningSeries = self.seriesStorage.insert_output(series)

        return returningSeries

    def request_input(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series:
        """This method attempts to return a series matching a series description and a time description.
            It will attempt first to get the series from series storage, kicking off data ingestion if series storage
            doesn't have all the data.
            NOTE: If an interval is not provided in the time description, the isComplete flag will always be false unless its a single data point (toTime = fromTime)
            :param seriesDescription - A description of the wanted series.
            :param timeDescription - A description of the temporal information of the wanted series. 
            :returns series - The series containing as much data as could be found.
        """
        
        #if the to and from time are the same, this is a single data point
        isSingleDataPoint = timeDescription.toDateTime == timeDescription.fromDateTime

        # Create the series that will be returned
        responseSeries = Series(seriesDescription, True, timeDescription)
        
        # Attempt to pull request from series storage.
        seriesStorageResults = self.seriesStorage.select_input(seriesDescription, timeDescription).data

        # Calculate how many results we are expecting, if this is a single value, it should only be 1 no matter if they provided and interval or not
        amountOfExpectedResults = 1 if isSingleDataPoint else self.__get_amount_of_results_expected(timeDescription)

        # If we can validate that series storage had all the data we needed, we just return it.
        if amountOfExpectedResults != None and len(seriesStorageResults) == amountOfExpectedResults:
            responseSeries.data = seriesStorageResults
            return responseSeries
        
        else: #Else we need to call data ingestion.

            # Call Data Ingestion to fetch data
            dataIngestion = data_ingestion_factory(seriesDescription)
            dataIngestionSeries = dataIngestion.ingest_series(seriesDescription, timeDescription)

            # If for some reason data ingestion fails we just return series storage results.
            if dataIngestionSeries is None:
                responseSeries.isComplete = False
                responseSeries.nonCompleteReason = 'Series storage results could not be verified, dataIngestion returned None'
                responseSeries.data = seriesStorageResults
                return responseSeries
            
            mergedResults = self.__merge_results(seriesStorageResults, dataIngestionSeries.data)
            responseSeries.data = mergedResults # At this point this is the max possible results we can return.
            
            # Second AmountCheck
            if amountOfExpectedResults == None: 

                if(len(mergedResults) > 1):
                    responseSeries.isComplete = True
                    responseSeries.nonCompleteReason = 'Result completeness could not be verified. (Did you forget to assign an Interval?)'
                return responseSeries
            elif len(mergedResults) != amountOfExpectedResults:
                responseSeries.isComplete = False
                responseSeries.nonCompleteReason = f'Combined data ingestion and series storage results were more or less than expected. Number of Results: {len(mergedResults)} | Number of Expected: {amountOfExpectedResults}'
                return responseSeries
            else: # This means the data is validated to be whole
                return responseSeries
            
    
    def request_output(self, semaphoreSeriesDescription: SemaphoreSeriesDescription, timeDescription: TimeDescription) -> Series: 
        ''' Takes a description of an output series and attempts to return it
            :param seriesDescription: SemaphoreSeriesDescription -A semaphore series description
            :param timeDescription: TimeDescription - A description about the temporal parts of the output series
            :return series
        '''
        
        ###See if we can get the outputs from the database
        requestedSeries = self.seriesStorage.select_output(semaphoreSeriesDescription, timeDescription)
        return requestedSeries
        
    
    def __get_amount_of_results_expected(self, timeDescription: TimeDescription) -> int | None:
        """ Calculates the amount of records we should expect give back
            :param timeDescription: TimeDescription - The temporal part of the request
            :return int: The amount of results expected
        """

        if timeDescription.interval == None: return None

        totalSecondsRequested = (timeDescription.toDateTime - timeDescription.fromDateTime).total_seconds()

        # We add one to acount for this being an inclusive time range. A time range from 12pm - 2pm includes both 12pm and 2pm.
        return 1 + totalSecondsRequested // timeDescription.interval.total_seconds()
    

    def __merge_results(self, first: list, second: list) -> list:
        """ Merges two lists together, will only keep unique results.
            :param first: list[any] - The first list.
            :param second: list[any] - The second list.
            :return list: The combined lists.
        """

        #TODO:: This is a very slow, it was optimized with hashing like this:    
            # uniqueToSecond = set(second) - set(first)
            # return first + list(uniqueToSecond)
        #But its not hashing properly anymore, needs to be looked into

        for item in second:
            if item not in first:
                first.append(item)
        return first

 
    