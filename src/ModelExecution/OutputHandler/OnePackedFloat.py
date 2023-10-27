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
from DataClasses import Prediction, SemaphoreSeriesDescription, Series
from ModelExecution.IOutputHandler import IOutputHandler



class OnePackedFloat(IOutputHandler):


    def post_process_prediction(self, prediction: Prediction) -> list[Prediction]:
        """Unpacks the prediction value before saving them to the db
        Parameters:
            predictionDesc: SemaphoreSeriesDescription
            prediction: Prediction
        Returns:
            The Response from the series provider 
        """
        unpackedPrediction = self.__unpack(prediction.value)

        predictions = []
        predictions.append(Prediction(unpackedPrediction, prediction.unit, prediction.leadTime, prediction.generatedTime))

        return predictions
    
    def __unpack(self, packedValue: any) -> any:
        """Flattens any dimensions of array and indexes and returns the first time, unpacking it.
        Parameters:
            prediction: any - the value to unpack. Should obv be arraylike.
        Returns:
            any - Whatever was inside    
        """
        return packedValue.ravel()[0]
