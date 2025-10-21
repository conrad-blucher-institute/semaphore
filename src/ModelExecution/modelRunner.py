# -*- coding: utf-8 -*-
#modelRunner.py
#----------------------------------
# Created By : Matthew Kastl
# Created Date: 2/3/2023
# version 1.0
#----------------------------------
""" This script houses the modelRunner class. The class wraps around Tenserflow and all 
Tenserflow related actions allowing us to run models from .H5
 """ 
#----------------------------------
# 
#
#Imports
from .IOutputHandler import output_handler_factory
from DataClasses import SemaphoreSeriesDescription, Series
from utility import log, construct_true_path
from exceptions import Semaphore_Exception
from ModelExecution.dspecParser import Dspec

import datetime
from os import path, getenv
from numpy import reshape
from tensorflow.keras.models import load_model


class ModelRunner:
     

    def make_predictions(self, DSPEC: Dspec, input_vectors: list[any], reference_time: datetime) -> Series:
        """Computes a prediction but does not save it to persistent storage"""

        log('Init load model....')
        model = self.__load_model(DSPEC.modelFileName)

        log('Init shape inputs....')
        # The input vector(s) must be reshaped to match the model's input shape
        # The first dimension of the expected shape is left None by TensorFlow 
        # for us to fill in the amount of input vectors we are providing.
        expectedShape = model.input_shape
        expectedShape = (len(input_vectors),) + expectedShape[1:] 
        shapedInputs = reshape(input_vectors, expectedShape) 

        log('Init compute prediction....')
        prediction =  model.predict(shapedInputs) 

        log('Init prediction post process....')
        #Instantiate the right output handler method then post process the predictions
        OH_Class = output_handler_factory(DSPEC.outputInfo.outputMethod)
        processedOutputs = OH_Class.post_process_prediction(prediction, DSPEC, reference_time) 

        #Put the post processed predictions in a series
        series = Series(
            description= SemaphoreSeriesDescription(
                modelName=      DSPEC.modelName, 
                modelVersion=   DSPEC.modelVersion, 
                dataSeries=     DSPEC.outputInfo.series, 
                dataLocation=   DSPEC.outputInfo.location, 
                dataDatum=      DSPEC.outputInfo.datum
            )
        )
        series.dataFrame= processedOutputs

        return series
    

    def __load_model(self, modelPath) -> None:
        """Private method to load a model saves as an h5 file designated
        in the dspec file using Tenserflow/Karas
        """
        h5FilePath = construct_true_path(getenv('MODEL_FOLDER_PATH')) + (modelPath if modelPath.endswith('.h5') else modelPath + '.h5')

        if not path.exists(h5FilePath): 
            raise Semaphore_Exception(f'H5 file for {modelPath} not found at {h5FilePath}!')
        
        return load_model(h5FilePath, compile=False)
    