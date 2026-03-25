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
import re
import datetime
from os import path, getenv
from numpy import reshape
import numpy as np
import glob
from tensorflow.keras.models import load_model


class ModelRunner:

    def make_predictions(self, DSPEC: Dspec, input_vectors: list[any], reference_time: datetime) -> Series:
        """
        This function runs predictions using one or multiple models and processes the results.

        - Loads model(s) based on the DSPEC configuration
        - Reshapes input vectors to match the expected model input shape
        - Runs predictions for each model using the same input data
        - Stacks all model predictions into a single 3D array 
        (models, input_vectors, outputs)
        - Applies post-processing using the specified output handler
        - Wraps the processed results into a Series object with metadata
        - Returns the final Series containing the prediction results
        """
        log('Init load model(s)....')

        models = self.__load_models(DSPEC)
        expectedMemberCount = DSPEC.outputInfo.expectedOutputShape.memberCount
        # Validate model count matches expected member count

        log(f"Loaded {len(models)} models, expected {expectedMemberCount}")

        if len(models) != expectedMemberCount:
            raise Semaphore_Exception(
                f"Expected {expectedMemberCount} model(s) based on DSPEC, but found {len(models)}"
            )
        
        log('Init shape inputs....')

        # Use first model for shape (all models should match)
        expectedShape = models[0].input_shape
        first_shape = models[0].input_shape
        
        for m in models:
            if m.input_shape != first_shape:
                raise Semaphore_Exception("Model input shapes do not match")
            
        expectedShape = (len(input_vectors),) + expectedShape[1:]
        shapedInputs = reshape(input_vectors, expectedShape)

        log('Init compute predictions for all models....')

        all_predictions = []

        for model in models:
            prediction = model.predict(shapedInputs)
            all_predictions.append(prediction)

        # Stack predictions → shape becomes (models, input_vectors, outputs)
        stacked_predictions = np.stack(all_predictions, axis=0)

        log('Init prediction post process....')

        OH_Class = output_handler_factory(DSPEC.outputInfo.outputMethod)
        processedOutputs = OH_Class.post_process_prediction(
            stacked_predictions, DSPEC, reference_time
        )

        series = Series(
            description=SemaphoreSeriesDescription(
                modelName=DSPEC.modelName,
                modelVersion=DSPEC.modelVersion,
                dataSeries=DSPEC.outputInfo.series,
                dataLocation=DSPEC.outputInfo.location,
                dataDatum=DSPEC.outputInfo.datum
            )
        )

        series.dataFrame = processedOutputs
        return series
    

    def __load_models(self, DSPEC: Dspec):
        """Loads either a single model or multiple models based on the Dspec"""
        
        base_path = construct_true_path(getenv('MODEL_FOLDER_PATH'))

        # Multi-model (CRPS)
        if DSPEC.modelFileNamePattern:
            pattern = base_path + DSPEC.modelFileNamePattern
            files = glob.glob(pattern)
            files.sort(key=self.extract_number)  # Ensure consistent ordering

            if not files:
                raise Semaphore_Exception(f"No models found for pattern: {pattern}")

            models = [load_model(f, compile=False) for f in files]
            return models

        # Single model
        elif DSPEC.modelFileName:
            model_path = DSPEC.modelFileName

            # Ensure .h5 extension
            if not model_path.endswith('.h5'):
                model_path += '.h5'

            full_path = base_path + model_path

            if not path.exists(full_path):
                raise Semaphore_Exception(
                    f'H5 file for {model_path} not found at {full_path}!'
                )

            model = load_model(full_path, compile=False)
            return [model]

        else:
            raise Semaphore_Exception(
                "No modelFileName or modelFileNamePattern provided"
            )
    
    def extract_number(self, filename):
        match = re.search(r'\d+', filename)
        return int(match.group()) if match else float('inf')

