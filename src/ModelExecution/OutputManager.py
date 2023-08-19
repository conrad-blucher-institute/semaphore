# -*- coding: utf-8 -*-
#OutputManager.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 7/12/2023
# version 1.0
#----------------------------------
"""This class is a class that maps
output save requests to their methods
and initiates the saving process.
 """ 
#----------------------------------
# 
#
#Imports
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))
from SeriesProvider.DataClasses import Prediction, SemaphoreSeriesDescription, Series
from SeriesProvider.SeriesProvider import SeriesProvider


from utility import log
from datetime import datetime

class OutputManager():

    def __init__(self) -> None:
        self.seriesProvider = SeriesProvider()

    def output_method_map(self, method: str, predictionDesc: SemaphoreSeriesDescription, predictions: Prediction | list[Prediction]) -> Series:
        """Maps a request to the specific method that handles its output.
        Paremeters:
            method: str - The string key to match to an output method
            predictionDesc: SemaphoreSeriesDescription - The description object holding all the info that the db will need to save it
            predictions: any | list[any] - The actual prediction(s) to save
        Returns:
            The inserted Series
        """
        
        match method:
                case 'one_packed_float':
                    return self.__one_packed_float(predictionDesc, predictions)
                case _:
                    log(f'No output method found for {method}!')
                    return None
    
    def __one_packed_float(self, predictionDesc: SemaphoreSeriesDescription, prediction: Prediction) -> Series:
        """Unpacks the prediction value before saving them to the db
        Paremeters:
            TODO IN refactor
        Returns:
            The Response from the seriesprovider 
        """
        unpackedPrediction = self.__unpack(prediction.value)

        predictions = []
        predictions.append(Prediction(unpackedPrediction, prediction.unit, prediction.leadTime, prediction.generatedTime))

        #Create a series to be sent to be stored in db
        request = Series(predictionDesc, True)
        request.bind_data(predictions)

        #Send the data to be stored in db
        return self.seriesProvider.make_SaveRequest(request)
    
    def __unpack(self, packedValue: any) -> any:
        """Flattens any deminsion of array and indexes and retruns the first time, unpacking it.
        Paremeters:
            prediction: any - the value to unpack. Should obv be arraylike.
        Returns:
            any - Whatever was inside    
        """
        return packedValue.ravel()[0]
