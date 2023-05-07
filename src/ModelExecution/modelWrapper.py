# -*- coding: utf-8 -*-
#modelWrapper.py
#----------------------------------
# Created By : Matthew Kastl
# Created Date: 2/3/2023
# version 1.0
#----------------------------------
""" This script houses the ModelWrapper class. The class functions to run an
AI model from an H5 file from a datetime and inputs specified in a dspec file.
 """ 
#----------------------------------
# 
#
#Imports
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

import datetime
from os import path
from utility import log
from ModelExecution.inputGatherer import InputGatherer
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
        modelName = self.__inputGatherer.get_model_name()
        h5FilePath = '../data/models/' + (modelName if modelName.endswith('.h5') else modelName + '.h5')

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
            shapedInputs = self.__shape_data(inputs) #Ensure recived inputs are shaped right for model
            return self._model.predict(shapedInputs) 
        except Exception as e:
            log(e)
            raise e