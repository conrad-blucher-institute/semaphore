# -*- coding: utf-8 -*-
#utility.py
#----------------------------------
# Created By : Matthew Kastl
# Created Date: 2/3/2023
# version 1.0
#----------------------------------
""" This file houses the ModelWrapper class. The class functions to run an
AI model from an AI file and call an inputGatherer to parse a dspecFilePath
and pull the inputs.
 """ 
#----------------------------------
# 
#
#Imports
from os import path
from utility import log
from tensorflow.keras.models import load_model
from inputGatherer import InputGatherer


class ModelWrapper:
    def __init__(self, h5FilePath: str, dspecFilePath: str) -> None:
        if not path.exists(h5FilePath):
            log("h5FilePath does not result in an existing file!")
            raise FileNotFoundError
        if not path.exists(dspecFilePath):
            log("dspecFilePath does not result in an existing file!")
            raise FileNotFoundError

        self._h5filePath = h5FilePath
        self.dspecFilePath = dspecFilePath


    def loadModele(self) -> None:
        try:
            self._model = load_model(self.h5FilePath)
            self._model.summary()
            self._model.get_weights()
        except Exception as e:
            log(e)


    def loadInputs(self) -> None:
        try:
            inputGatherer = InputGatherer(self._dspecFilePath)
            self._inputs = inputGatherer.pullInputs
        except Exception as e:
            log(e)

    def makePrediction(self) -> any:
        try:
            self.loadModele()
            self.loadInputs()
            return self._model.predict(self._inputs) 
        except Exception as e:
            log(e)