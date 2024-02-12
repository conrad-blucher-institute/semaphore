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
from SeriesStorage.ISeriesStorage import series_storage_factory
from DataIngestion.IDataIngestion import data_ingestion_factory
from DataClasses import Series, SemaphoreSeriesDescription, SeriesDescription, TimeDescription
from utility import log

from datetime import timedelta



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
            NOTE: If an interval is not provided in the time description, the interval will be assumed to be 6 minuets
            :param seriesDescription - A description of the wanted series.
            :param timeDescription - A description of the temporal information of the wanted series. 
            :returns series - The series containing as much data as could be found.
        """

        # If an interval was not provided we have to make an assumption to be able to validate it. Here we assume the interval to be 6 minuets
        timeDescription.interval = timedelta(minutes=6) if timeDescription.interval == None else timeDescription.interval
        
        # Pull data from series storage and validate it, if valid return it
        series_storage_results = self.seriesStorage.select_input(seriesDescription, timeDescription)
        validated_series_storage_results = self.__validate_results(seriesDescription, timeDescription, series_storage_results)
        if(validated_series_storage_results.isComplete):
            return validated_series_storage_results

        # If series storage results weren't valid
        # Pull data ingestion validate it with the series storage results, if valid return it
        data_ingestion_class = data_ingestion_factory(seriesDescription)
        data_ingestion_results = data_ingestion_class.ingest_series(seriesDescription, timeDescription)
        validated_merged_result = self.__validate_results(seriesDescription, timeDescription, series_storage_results, data_ingestion_results)
        if(validated_merged_result.isComplete):
            return validated_merged_result
        
        # If neither were valid then we log this occurrence. We still return the best result we have, which is the merged result 
        log(f'''Request input failure, 
                    Reason: {validated_merged_result.nonCompleteReason} \n 
                    {seriesDescription} \n 
                    {timeDescription} \n 
                    Series Storage Result: {series_storage_results.data} \n 
                    Data Ingestion Result: {None if data_ingestion_results == None else data_ingestion_results.data} \n
                    Merged Result: {validated_merged_result.data}
            ''')
        return validated_merged_result

            
    
    def request_output(self, semaphoreSeriesDescription: SemaphoreSeriesDescription, timeDescription: TimeDescription) -> Series: 
        ''' Takes a description of an output series and attempts to return it
            :param seriesDescription: SemaphoreSeriesDescription -A semaphore series description
            :param timeDescription: TimeDescription - A description about the temporal parts of the output series
            :return series
        '''
        
        ###See if we can get the outputs from the database
        requestedSeries = self.seriesStorage.select_output(semaphoreSeriesDescription, timeDescription)
        return requestedSeries


    def __validate_results(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, DBSeries: Series, DISeries: Series = None) -> Series: 
        """This function validates a set of results against a request. It will generate a list of every expected time stamp and 
        attempt to match inputs to those timestamps. If both a database and data ingestion series are provided it will merge them
        prioritizing results from data ingestion when they conflict with results from the database.
        :param seriesDescription: SemaphoreSeriesDescription - The request description
        :param timeDescription: TimeDescription - The description of the temporal information of the request 
        :param DBSeries: Series - A series of results to validate from the DB
        :param DISeries: Series - A series of results to validate from data ingestion
        :return a validated series: Series

        """
        missing_results = 0
        datetimeDict = self.__generate_datetime_map(timeDescription) # A dict with every time stamp we expect, with a value of none

        # Iterate over every timestamp we are expecting 
        for datetime in datetimeDict.keys():
            
            # Search the DB results for every input that matches that timestamp
            valid_database_results = [input for input in DBSeries.data if input.timeGenerated == datetime]
            if len(valid_database_results) > 1:
                log(f'Warning: DB results returned multiple results for the same datetime \n{DBSeries}\n{valid_database_results}')

            # Search the DI results for every input that matches that timestamp if there are DI results
            if DISeries != None:
                valid_ingestion_results = [input for input in DISeries.data if input.timeGenerated == datetime]
                if len(valid_ingestion_results) > 1:
                    log(f'Warning: DataIngestion results returned multiple results for the same datetime \n{DISeries}\n{valid_ingestion_results}')

            # If there are no results from either DB or DI thats missing, 
            # if there are results we prioritize the DI result as thats assumed to be more accurate
            DB_result_exists = len(valid_database_results) > 0
            DI_result_exits = DISeries != None and len(valid_ingestion_results) > 0

            if not DB_result_exists and not DI_result_exits:
                missing_results = missing_results + 1
            elif DI_result_exits:
                datetimeDict[datetime] = valid_ingestion_results[0]
            else: 
                datetimeDict[datetime] = valid_database_results[0]

        # If there were missing results we assign as such in the series
        # No log here, as this method will be used to also detect if Data Ingestion should be kicked off
        isComplete = True
        reason_string = ''
        if missing_results > 0:
            isComplete = False
            reason_string = f'There were {missing_results} missing results!'

        result = Series(
            description= seriesDescription, 
            isComplete= isComplete,
            timeDescription=timeDescription,
            nonCompleteReason=reason_string
        )
        result.data = list(datetimeDict.values())
        return result
                
 
    def __generate_datetime_map(self, timeDescription: TimeDescription) -> dict:
        """This function creates a dictionary of expected time stamps between a from time and a two time at some interval.
        The keys are the time steps and the values are always set to None.
        If to time and from time are equal, its only a single pair is returned as that describes a single input.
        :param timeDescription: TimeDescription - The description of the temporal information of the request 
        :return dict{datetime : None}
        """
        # If to time == from time this is a request for a single point
        if timeDescription.fromDateTime == timeDescription.toDateTime:
            return {timeDescription.fromDateTime : None}
        
        # Define the initial time and how many time steps their are.
        initial_time = timeDescription.fromDateTime
        steps = int((timeDescription.toDateTime - timeDescription.fromDateTime) / timeDescription.interval)
        steps = steps + 1 # We increment here as we are inclusive on both sides of the range of datetimes [from time, to time]
        
        # GenerateTimeStamp will calculate a timestamp an amount of steps away from the initial time
        generateTimestamp = lambda initial_time, idx, interval : initial_time + (interval * idx)

        # Preform list comprehension to generate a list of all the time steps we need
        keys = [generateTimestamp(initial_time, idx, timeDescription.interval) for idx in range(steps)]
        # Generate a list of nones the same length
        values = [None] * len(keys)

        # zip both lists in a dictionary and return the result
        return { k:v for (k, v) in zip(keys, values)}
    