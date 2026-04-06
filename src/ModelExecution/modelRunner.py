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
from tensorflow.keras.models import load_model, Model


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

        if len(models) != expectedMemberCount:
            raise Semaphore_Exception(
                f"Expected {expectedMemberCount} model(s) based on DSPEC, but found {len(models)}"
            )
        
        log('Init shape inputs....')

        # Use first model for shape (all models should match)
        model_input_shape = models[0].input_shape
        expectedShape = (len(input_vectors),) + model_input_shape[1:]
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
    

    def __load_models(self, DSPEC: Dspec) -> list[Model]:
        """
        This function will construct the model path based on the dspec and will 
        to load all models that match the path.

        :param DSPEC: Dspec - The Dspec file with the model loading information

        :returns list - A list of loaded models.
            for single member models the list will have 1 model, [model]
            for multi member models the list will have multiple models sorted by member index [member1, member2, ...]
        """
        
        base_path = construct_true_path(getenv('MODEL_FOLDER_PATH'))
        model_path = base_path + DSPEC.modelFileName

        # if the model path doesn't end with .h5 or .keras, we assume .h5 and append it
        if not (model_path.endswith('.h5') or model_path.endswith('.keras')):
            model_path = model_path + '.h5'

        # use glob to find all matching files
        # this will return a list of all found models
        # or an empty list if no models are found
        model_files = glob.glob(model_path)

        if not model_files:
            raise Semaphore_Exception(f"No model file(s) found for path: {model_path}")
        
        # if multiple models are found, sort to ensure consistent loading order (member1, member2, ...)
        if len(model_files) > 1:
            model_files.sort(key=self.extract_number)
        

        first_model = load_model(model_files[0], compile=False)
        
        expected_shape = first_model.input_shape

        loaded_models = [first_model]
        
        for file in model_files[1:]:
            model = load_model(file, compile=False)

            
            if model.input_shape != expected_shape:
                raise Semaphore_Exception(
                    f"Model input shape mismatch for file {file}: "
                    f"expected {expected_shape}, got {model.input_shape}"
                )
            
            
            loaded_models.append(model)

        return loaded_models
    
    def extract_number(self, filename):
        """
        This function extracts the member index from a filename for consistent model loading order.
        It matches the pattern 'member<N>' so 'model_120hr_member3' -> 3.
        If no member pattern is found, infinity is returned to sort those files at the end.

        This is used by files.sort() to ensure model members are loaded in the correct order
        (member1, member2, ...)

        :param filename: str - The filename to extract the member number from.

        :returns int - The extracted member number on successful matches
        :returns float('inf') - If no member pattern is found, returns infinity to sort that file at the end
        """
        match = re.search(r'member(\d+)', filename, re.IGNORECASE)
        return int(match.group(1)) if match else float('inf')

