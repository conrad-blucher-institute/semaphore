# -*- coding: utf-8 -*-
#inputGatherer.py
#----------------------------------
# Created By : Matthew Kastl
# Created Date: 2/3/2023
# version 1.0
#----------------------------------
""" This file houses the InputGathere class, it houses funtion and methods of parsing
the dspec file and pulling inputs from different places.
 """ 
#----------------------------------
# 
#
#Input
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from DataManagement.DataManager import DataManager
from DataManagement.DataClasses import Request, Response, DataPoint, Prediction

from os import path, getenv
from utility import log, construct_true_path
from json import load
from csv import reader
from datetime import datetime, timedelta

class InputGatherer:
    def __init__(self, dspecFileName: str) -> None:
        """Constructor sends the specFile off to be loaded and parsed
        """
        self.__parse_dspec(dspecFileName)
        self.__dataManager = DataManager()

    def get_dataManager(self):
        return self.__dataManager
    
    def __parse_dspec(self, dspecFileName: str) -> None:
        """Loads a dspec as a JSON file and parses out the options and input sepcifications. 
        It saves them as private class objects.
        """

        dspecFilePath = construct_true_path(getenv('DSPEC_FOLDER_PATH')) + dspecFileName

        if not path.exists(dspecFilePath):
            log(f'{dspecFilePath} not found!')
            raise FileNotFoundError
        
        with open(dspecFilePath) as dspecFile:
            self.__dspecDict = load(dspecFile)
            self.__outputInfo = self.__dspecDict['outputInfo']
            self.__inputSpecifications = self.__dspecDict['inputs']

    def __create_request(self, spec: dict, now: datetime):
        span = spec["between"]

        toDateTime = now + timedelta(hours= span[0])
        fromDateTime = now + timedelta(hours= span[1])

        #TODO:: Create better logic to propperly analyse a given input
        isOnePoint = span[0] == span[1]
        if isOnePoint:
            fromDateTime = fromDateTime.replace(minute=0, second=0, microsecond=0)

        return Request(spec['source'], spec['series'], spec['location'], spec['unit'], fromDateTime, toDateTime, spec.get('datum'))
    
    def get_model_name(self) -> str:
        """Returns the name of the model as specified in the DSPEC file."""
        return self.__dspecDict['modelName']       

    def get_inputs(self, dateTime: datetime) -> list[any]:
        """Public method that reads the import method from the dspec file and starts execution to
        gather said inputs. Returns the inputs as an array.
        """
        
        inputVector = []
        for specification in self.__inputSpecifications:
                
            try:
                response = None
                request = self.__create_request(specification, dateTime)
                response = self.__dataManager.make_request(request)

                for data in response.data:
                    
                    #Cast from string to unit then append
                    match data.unit:
                        case 'float':
                            inputVector.append(float(data.value))
                        case _:
                            log(f'Input gatherer has no conversion for Unit: {data.unit}')

            except Exception as e:
                log(f'ERROR: There was a problem in the input gatherer gathering a sepcification.\n\n Specification= {specification}\n\n Response= {response}\n\n Error={e}')
                return -1

        return inputVector

    
    def get_outputInfo(self) -> dict:
        return self.__outputInfo
    
    def get_dspec(self) -> dict:
        return self.__dspecDict
