# -*- coding: utf-8 -*-
#modelWrapper.py
#----------------------------------
# Created By : Matthew Kastl
# Created Date: 2/3/2023
# version 1.0
#----------------------------------
""" This script houses the ModelWrapper class. The class wrapps around tenserflow and all 
tenserflow related actions allowing us to run models from .H5
 """ 
#----------------------------------
# 
#
#Imports
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from ModelExecution.inputGatherer import InputGatherer
from ModelExecution.OutputManager import OutputManager
from SeriesProvider.DataClasses import SemaphoreSeriesDescription, Prediction

import datetime
from os import path, getenv
from utility import log, construct_true_path
from numpy import array, reshape
from tensorflow.keras.models import load_model

class ModelWrapper:
    def __init__(self, dspecFileName: str) -> None:
        """Constructor generates an InputGatherer parse the dspec file 
        and attempts to load the model.
        """
        self.__inputGatherer = InputGatherer(dspecFileName)
        self.__load_modele()


    def __load_modele(self) -> None:
        """Private method to load a model saves as an h5 file designated
        in the dspec file using Tenserflow/Karas
        """
        modelName = self.__inputGatherer.get_model_file_name()
        h5FilePath = construct_true_path(getenv('MODEL_FOLDER_PATH')) + (modelName if modelName.endswith('.h5') else modelName + '.h5')

        if not path.exists(h5FilePath): 
            log(f'H5 file for {modelName} not found at {h5FilePath}!')
            raise FileNotFoundError

        try:
            self._model = load_model(h5FilePath)
        except Exception as e:
            log(e) 
            raise e

    
    def __shape_data(self, inputs: array) -> None:
        """Private method to convert the shape  of the data from the inputGatherer, 
        to the correct shape required by the model, dynamically. The shape that the input comes in
        and what the model was trained with, aren't nessisarily the same.
        """

        #Get only first and last layers
        firstLayer, *_, lastLayer = self._model.layers 

        #Karas reports the Batch size as None in the shape; Convert None -> 1, 
        #We only want one prediction as opposed to a batch.
        shapeTarget = [int(1 if value is None else value) for value in firstLayer.input_shape]

        #Reshape and return the data.
        return reshape(inputs, shapeTarget) 
    

    def make_prediction(self, dateTime: datetime) -> any:
        """Public method to generate a prediction given a datetime.
        """
        try:
            inputs = self.__inputGatherer.get_inputs(dateTime)
            if inputs == -1: return -1
            
            shapedInputs = self.__shape_data(inputs) #Ensure recived inputs are shaped right for model
            return self._model.predict(shapedInputs) 
        except Exception as e:
            log(e)
            raise e
        
    def make_and_save_prediction(self, dateTime: datetime) -> any:
        """Public method to generate a prediction given a datetime.
        """
        try:
            inputs = self.__inputGatherer.get_inputs(dateTime)
            if inputs == -1: return -1
            
            shapedInputs = self.__shape_data(inputs) #Ensure recived inputs are shaped right for model
            prediction =  self._model.predict(shapedInputs) 
            dspec = self.__inputGatherer.get_dspec()
            outputInfo = dspec.outputInfo

            #TODO::This code will need to be reworded we we get more than one value from a mdel
            predecitionDesc = SemaphoreSeriesDescription(dspec.modelName, dspec.modelVersion, outputInfo.series, outputInfo.location, outputInfo.datum)
            prediction = Prediction(prediction, outputInfo.unit, outputInfo.leadTime, dateTime)

            om = OutputManager()
            om.output_method_map(outputInfo.outputMethod, predecitionDesc, prediction)
            return prediction

        except Exception as e:
            log(e)
            raise e