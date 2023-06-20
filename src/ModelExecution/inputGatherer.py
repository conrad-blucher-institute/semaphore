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

from os import path
from utility import log
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

        dspecFilePath = '../data/dspec/' + dspecFileName

        if not path.exists(dspecFilePath):
            log(f'{dspecFilePath} not found!')
            raise FileNotFoundError
        
        with open(dspecFilePath) as dspecFile:
            self.__dspecDict = load(dspecFile)
            optionList = self.__dspecDict['options']
            self.__inputSpecifications = self.__dspecDict['inputs']

        #Combine every opetion into one "options dict"
        self.__options = dict()
        for dictionary in optionList:
            for key, value in dictionary.items():
                self.__options[key] = value

    def __create_request(self, spec: dict, now: datetime):
        span = spec["between"]
        
        toDateTime = now + timedelta(hours= span[0])
        fromDateTime = now + timedelta(hours= span[1])
        print(f'{now} - {span[0]} | {fromDateTime} - {span[1]} | {toDateTime}')
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
            request = self.__create_request(specification, dateTime)
            print(request)
        #     response = self.__dataManager.make_request(request)
            
        #     for data in response.data:
        #         inputVector.append(data)

        # print(inputVector)
        assert False

    
    def get_options(self) -> dict:
        return self.__options

