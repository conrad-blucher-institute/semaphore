# -*- coding: utf-8 -*-
#modelWrapper.py
#----------------------------------
# Created By : Matthew Kastl
# Created Date: 2/3/2023
# version 1.0
#----------------------------------
""" This script houses the ModelWrapper class. The class wraps around Tenserflow and all 
Tenserflow related actions allowing us to run models from .H5
 """ 
#----------------------------------
# 
#
#Imports


from .inputGatherer import InputGatherer
from .IOutputHandler import output_handler_factory
from DataClasses import SemaphoreSeriesDescription, Series
from utility import log, construct_true_path
from exceptions import Semaphore_Exception

import datetime
from os import path, getenv
from numpy import array, reshape
from tensorflow.keras.models import load_model


class ModelWrapper:
    def __init__(self, inputGatherer: InputGatherer) -> None:
        """
        Uses the source attribute of a data request to dynamically import a module
        ------
        Parameters
        request: SeriesDescription - A data SeriesDescription object with the information to pull (src/DataManagment/DataClasses>Series)
        Returns
        IDataIngestion - An child of the IDataIngestion interface.
        """
        """Constructor generates an InputGatherer parse the dspec file 
        and attempts to load the model.
        """
        self.__inputGatherer = inputGatherer
        self.__load_model()
       

    def __load_model(self) -> None:
        """Private method to load a model saves as an h5 file designated
        in the dspec file using Tenserflow/Karas
        """
        modelName = self.__inputGatherer.get_model_file_name()
        h5FilePath = construct_true_path(getenv('MODEL_FOLDER_PATH')) + (modelName if modelName.endswith('.h5') else modelName + '.h5')

        if not path.exists(h5FilePath): 
            log(f'H5 file for {modelName} not found at {h5FilePath}!')
            raise FileNotFoundError

        try:
            self._model = load_model(h5FilePath, compile=False)
        except Exception as e:
            log(e) 
            raise e

    
    def __shape_data(self, inputs: array) -> None:
        """Private method to convert the shape  of the data from the inputGatherer, 
        to the correct shape required by the model, dynamically. The shape that the input comes in
        and what the model was trained with, aren't necessarily the same.
        """
      
        
        shape = self._model.input_shape
        
        
        if type(shape) == list:
            shape = shape[0]

        if type(shape) != tuple:
            raise TypeError(f'First layer input shape was not a tuple, instead: {type(shape)}')
        
        if int(shape[-1]) != len(inputs):
            raise ValueError(f'Incorrect amount of inputs to shape. Desired: {shape[-1]} Received: {len(inputs)}')

        #Karas reports the Batch size as None in the shape; Convert None -> 1, 
        #We only want one prediction as opposed to a batch.
        shapeTarget = [int(1 if value is None else value) for value in shape]

        #Reshape and return the data.
        return reshape(inputs, shapeTarget) 
    

    def make_prediction(self, dateTime: datetime) -> Series:
        """Computes a prediction but does not save it to persistent storage"""
        #converting execution time to reference time
        referenceTime = self.__inputGatherer.calculate_referenceTime(dateTime)
            
        log('Init get inputs....')
        inputs = self.__inputGatherer.get_inputs(referenceTime)

        if inputs == -1 or len(inputs) == 0: 
            raise Semaphore_Exception('Error:: No inputs received for model.')        

        log('Init shape inputs....')
        shapedInputs = self.__shape_data(inputs) #Ensure received inputs are shaped right for model

        log('Init compute prediction....')
        prediction =  self._model.predict(shapedInputs) 

        log('Init prediction post process....')
        dspec = self.__inputGatherer.get_dspec()
        outputInfo = dspec.outputInfo

        predictionDesc = SemaphoreSeriesDescription(dspec.modelName, dspec.modelVersion, outputInfo.series, outputInfo.location, outputInfo.datum)

        #Instantiate the right output handler method then post process the predictions
        OH_Class = output_handler_factory(outputInfo.outputMethod)
        processedOutputs = OH_Class.post_process_prediction(prediction, dspec, referenceTime) 

        #Put the post processed predictions in a series
        series = Series(predictionDesc, True)
        series.data = processedOutputs

        return series