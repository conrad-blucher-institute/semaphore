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
from os import path
from utility import log
from tensorflow.keras.models import load_model
from inputGatherer import InputGatherer
import datetime


class ModelWrapper:
    def __init__(self, dspecFilePath: str) -> None:
        """Constructor generates an InputGatherer parse the dspec file 
        and attempts to load the model.
        """
        self.__inputGatherer = InputGatherer(self._dspecFilePath)
        self.__load_modele()


    def __load_modele(self) -> None:
        """Private method to load a model saves as an h5 file designated
        in the dspec file using Tenserflow/Karas
        """
        modelName = self.__inputGatherer.get_model_name()
        h5FilePath = modelName if modelName.endswith('.h5') else modelName + '.h5'

        if not path.exists(h5FilePath): 
            log(f'H5 file not found!')
            raise FileNotFoundError

        try:
            self._model = load_model(h5FilePath)
            self._model.summary() #TODO::check to remove
            self._model.get_weights() #TODO::check to remove
        except Exception as e:
            log(e) 
            raise e


    def make_prediction(self, dateTime: datetime) -> any:
        """Public method to generate a prediction given a datetime.
        """
        try:
            self.__inputs = self.inputGatherer.get_inputs(dateTime)
            return self._model.predict(self.__inputs) 
        except Exception as e:
            log(e)
            raise e