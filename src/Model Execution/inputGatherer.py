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
from os import path
from utility import log
from json import load
from csv import reader
import datetime

class InputGatherer:
    def __init__(self, dspecFilePath: path) -> None:
        """Constructor sends the specFile off to be loaded and parsed
        """
        self.__parse_dspec(dspecFilePath)
    
    def __parse_dspec(self, dspecFilePath: path) -> None:
        """Loads a dspec as a JSON file and parses out the options and input sepcifications. 
        It saves them as private class objects.
        """
        if not path.exists(dspecFilePath):
            log(f'dspecFilePath not found!')
            raise FileNotFoundError
        
        with open(dspecFilePath) as dspecFile:
            self.__dspecDict = load(dspecFile)
            self.__options = self.__dspecDict['options']
            self.__inputSpecification = self.__dspecDict['inputs']
    
    def __pull_inputs_from_CSV(self) -> list[any]:
        """Pulls inputs from a CSV path. Assumes one row and no headers. Returns every column.
        """
        if not path.exists(self.__options['importPath']): 
            log(f'CVS path not valid for {self.__dspecDict["Name"]}!')
            raise FileNotFoundError

        with open(self.__options['importPath'], newline= '') as csvfile:
            return list(reader(csvfile))[0]
    
    def get_model_name(self) -> str:
        """Returns the name of the model as specified in the DSPEC file."""
        return self.__dspecDict['modelName']       

    def get_inputs(self, dateTime: datetime) -> list[any]:
        """Public method that reads the import method from the dspec file and starts execution to
        gather said inputs. Returns the inputs as an array.
        """
        #TODO::Handle for requested date
        match self.__options['importMethod']:
            case 'fromLocalSCV': return self.__pull_inputs_from_CSV()
            case _:
                log(f'Failed to catch importMethod from dspec {self.__dspecDict["Name"]}!')
                raise SyntaxError

