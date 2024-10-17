# -*- coding: utf-8 -*-
#TEST_SS.py
#-------------------------------
# Created By : Matthew Kastl
# Created Date: 9/3/2024
# version 1.0
#-------------------------------
""" This file is a mock series storage class used to test other parts of SEMAPHORE. 
To utilize this class you will need to set the env 'ISERIESSTORAGE_INSTANCE' = 'TEST_SS'
You must also in your request have the data source be 'TEST_SS'
        NOTE:: YOU MUST CHANGE IT BACK IF THIS CODE IS TO BE TESTED LIVE! 
The behavior of the functions is determined by what you set the dataSeries to in the description:

    COMPLETE - A series that is correct according to your time description
    MISS_# - A series is correct but is missing a # of values (Make sure your time description covers enough time ) These values will be randomly selected
    MISS_FIRST - A series that is correct but missing first value
    MISS_LAST - A series that is correct but missing last value
    ERROR_NO_RAISE - Basically just returns None as if the ingestion class encountered an error
    ERROR_RAISE - Actually will raise an exception for you to catch
 """ 
#-------------------------------
# 
#
#Imports
from SeriesStorage.ISeriesStorage import ISeriesStorage
from DataClasses import Series, SeriesDescription, SemaphoreSeriesDescription, Input, Output, TimeDescription
from utility import log
import random
from datetime import datetime


class TEST_SS(ISeriesStorage):

    def __init__(self):
        log('WARNING:: TEST_SS a testing series storage class just got invoked!')
 
    def select_input(self, seriesDescription: SeriesDescription, timeDescription : TimeDescription) -> Series:
        if seriesDescription.dataSource != 'TEST_SS': # Make sure we only catch series we are supposed to provide
            return Series(seriesDescription, False, timeDescription)

        match seriesDescription.dataSeries:
            case 'COMPLETE': 
                return self.__make_complete_series(seriesDescription, timeDescription)
            case 'MISS_FIRST':
                series = self.__make_complete_series(seriesDescription, timeDescription)
                series.data = series.data[1:]
                return series
            case 'MISS_LAST':
                series = self.__make_complete_series(seriesDescription, timeDescription)
                series.data = series.data[:-1]
                return series
            case 'ERROR_NO_RAISE':
                return None
            case 'ERROR_RAISE':
                raise Exception
            case _:
                isMiss = seriesDescription.dataSeries.find('MISS_') != -1
                if not isMiss: 
                    raise Exception(f'{seriesDescription.dataSeries} requested in SS testing class but couldn\'t be understood')

                amount_of_missing = int(seriesDescription.dataSeries[5:])
                series = self.__make_complete_series(seriesDescription, timeDescription)

                if len(amount_of_missing > len(series.data)):
                    raise Exception(f'In testing SS class {amount_of_missing} from a series that only has {len(series.data)} values!')
                
                series.data = self.__generate_data_with_missing(series.data, amount_of_missing)
                return series

    
    def select_output(self, semaphoreSeriesDescription: SemaphoreSeriesDescription, timeDescription : TimeDescription) -> Series:
        match semaphoreSeriesDescription.dataSeries:
            case 'COMPLETE': 
                return self.__make_complete_series(semaphoreSeriesDescription, timeDescription, False)
            case 'MISS_FIRST':
                series = self.__make_complete_series(semaphoreSeriesDescription, timeDescription, False)
                series.data = series.data[1:]
                return series
            case 'MISS_LAST':
                series = self.__make_complete_series(semaphoreSeriesDescription, timeDescription, False)
                series.data = series.data[:-1]
                return series
            case 'ERROR_NO_RAISE':
                return None
            case 'ERROR_RAISE':
                raise Exception
            case _:
                isMiss = semaphoreSeriesDescription.dataSeries.find('MISS_') != -1
                if not isMiss: 
                    raise Exception(f'{semaphoreSeriesDescription.dataSeries} requested in SS testing class but couldn\'t be understood')

                amount_of_missing = int(semaphoreSeriesDescription.dataSeries[5:])
                series = self.__make_complete_series(semaphoreSeriesDescription, timeDescription, False)

                if amount_of_missing > len(series.data):
                    raise Exception(f'In testing SS class {amount_of_missing} from a series that only has {len(series.data)} values!')
                
                series.data = self.__generate_data_with_missing(series.data, amount_of_missing)
                return series
    
    def find_external_location_code(self, sourceCode: str, location: str, priorityOrder: int = 0) -> str:
        return 'TEST'

    def find_lat_lon_coordinates(self, locationCode: str) -> tuple:
        return (0, 0)
    
    def insert_input(self, series: Series) -> Series:
        log(f'{len(series.data)} received by ORM to be inserted as inputs')
        return series
    
    def insert_output(self, series: Series) -> Series:
        log(f'{len(series.data)} received by ORM to be inserted as outputs')
        return series
    

    def __generate_data_with_missing(self, data: list[Input], amount_of_missing: int) -> list[Input]:
        missing_idx = random.sample(range(0, len(data)), amount_of_missing)

        new_data = []
        for idx, data in enumerate(data):
            if idx in missing_idx:
                continue
            else:
                new_data.append(data)

        return new_data

            
    def __make_complete_series(self, seriesDescription: SeriesDescription | SemaphoreSeriesDescription, timeDescription: TimeDescription, isInput: bool = True) -> Series:
        
        date_times = self.__generate_datetime_list(timeDescription)

        data = []
        for datetime in date_times:
            if isInput:
                data.append(Input(0, 'TEST', datetime, datetime, 'TEST', 'TEST'))
            else:
                data.append(Output(0, 'TEST', datetime,timeDescription.interval))

        series = Series(seriesDescription, False, timeDescription)
        series.data = data
        return series

    def __generate_datetime_list(self, timeDescription: TimeDescription) -> list:
        """This function creates a list of expected time stamps between a from time and a two time at some interval.
        The keys are the time steps and the values are always set to None.
        If to time and from time are equal, its only a single pair is returned as that describes a single input.
        :param timeDescription: TimeDescription - The description of the temporal information of the request 
        :return list[datetime]
        """
        # If to time == from time this is a request for a single point
        if timeDescription.fromDateTime == timeDescription.toDateTime:
            return [timeDescription.toDateTime]
        
        if timeDescription.interval.total_seconds() == 0:
            return [timeDescription.toDateTime]
        
        # Define the initial time and how many time steps their are.
        initial_time = timeDescription.fromDateTime
        steps = int((timeDescription.toDateTime - timeDescription.fromDateTime) / timeDescription.interval)
        steps = steps + 1 # We increment here as we are inclusive on both sides of the range of datetimes [from time, to time]
        
        # GenerateTimeStamp will calculate a timestamp an amount of steps away from the initial time
        generateTimestamp = lambda initial_time, idx, interval : initial_time + (interval * idx)

        # Perform list comprehension to generate a list of all the time steps we need plus another list of the same size this is all None
        return [generateTimestamp(initial_time, idx, timeDescription.interval) for idx in range(steps)]
            

    def insert_output_and_model_run(self, output_series: Series, execution_time: datetime, return_code: int) -> tuple[Series, dict]:
        raise NotImplementedError()
    
    def select_latest_output(self, model_name: str, from_time: datetime, to_time: datetime) -> Series | None: 
        raise NotImplementedError()
