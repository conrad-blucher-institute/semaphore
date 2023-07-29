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
from SeriesProvider.DataClasses import Prediction, SaveRequest, Response
from SeriesProvider.SeriesProvider import DataManager


from utility import log
from datetime import datetime

class OutputManager():

    def __init__(self) -> None:
        self.dataManager = DataManager()

    def output_method_map(self, method, prediction, AIName: str, generatedTime: datetime, leadTime: float, AIGeneratedVersion: str, series: str, location: str, datum: str = None, latitude: str = None, longitude: str = None) -> Response:
        """Maps a request to the specific method that handles its output.
        Paremeters:
            TODO IN refactor
        Returns:
            The Response from the datamanager
        """
        
        match method:
                case 'one_packed_float':
                    return self.__one_packed_float( prediction, AIName, generatedTime, leadTime, AIGeneratedVersion, series, location, datum, latitude, longitude)
                case _:
                    log(f'No output method found for {method}!')
                    return None
    
    def __one_packed_float(self, prediction, ModelName: str, generatedTime: datetime, leadTime: float, ModelVersion: str, series: str, location: str, datum: str = None, latitude: str = None, longitude: str = None) -> Response:
        """Unpacks the prediction value before saving them to the db
        Paremeters:
            TODO IN refactor
        Returns:
            The Response from the datamanager 
        """
        prediction = self.__unpack(prediction)

        predictions = []
        predictions.append(Prediction(prediction, 'float', leadTime, generatedTime))
        request = SaveRequest(ModelName, ModelVersion, series, location, datum)
        request.bind_predictions(predictions)
        return self.dataManager.make_SaveRequest(request)
    
    def __unpack(self, packedValue: any) -> any:
        """Flattens any deminsion of array and indexes and retruns the first time, unpacking it.
        Paremeters:
            prediction: any - the value to unpack. Should obv be arraylike.
        Returns:
            any - Whatever was inside    
        """
        return packedValue.ravel()[0]
