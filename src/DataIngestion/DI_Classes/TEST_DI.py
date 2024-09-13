# -*- coding: utf-8 -*-
#TEST_DI.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 9/3/2024
# version 1.0
#----------------------------------
""" This file is a mock ingestion class used to test other parts of SEMAPHORE. 
To utilize this class for testing, simply write a series description with the data
source set to: TEST_DI and the series set to one of the below that you want:

COMPLETE - A series that is correct according to your time description
MISS_# - A series is correct but is missing a # of values (Make sure your time description covers enough time ) These values will be randomly selected
MISS_FIRST - A series that is correct but missing first value
MISS_LAST - A series that is correct but missing last value
ERROR_NO_RAISE - Basically just returns None as if the ingestion class encountered an error
ERROR_RAISE - Actually will raise an exception for you to catch
 """ 
#----------------------------------
# 
#
#Input
from DataClasses import Series, SeriesDescription, Input, TimeDescription
from DataIngestion.IDataIngestion import IDataIngestion

import random

class TEST_DI(IDataIngestion):
    
    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
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
                    raise Exception(f'{seriesDescription.dataSeries} requested in DI testing class but couldn\'t be understood')

                amount_of_missing = int(seriesDescription.dataSeries[5:])
                series = self.__make_complete_series(seriesDescription, timeDescription)

                if amount_of_missing > len(series.data):
                    raise Exception(f'In testing DI class {amount_of_missing} from a series that only has {len(series.data)} values!')
                
                series.data = self.__generate_data_with_missing(series.data, amount_of_missing)
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

            
    def __make_complete_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series:
        
        date_times = self.__generate_datetime_list(timeDescription)

        data = []
        for datetime in date_times:
            data.append(Input(0, 'TEST', datetime, datetime, 'TEST', 'TEST'))
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
            
    