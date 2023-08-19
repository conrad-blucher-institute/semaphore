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
#Imports
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from SeriesProvider.SeriesProvider import SeriesProvider
from SeriesProvider.DataClasses import SeriesDescription, Series, Actual, Prediction
from ModelExecution.dspec import Dspec, OutputInfo, Input


from os import path, getenv
from utility import log, construct_true_path
from json import load
from csv import reader
from datetime import datetime, timedelta

from typing import List


class InputGatherer:
    def __init__(self, dspecFileName: str) -> None:
        """Constructor sends the specFile off to be loaded and parsed
        """
        self.__dspec = None
        self.__specifications = None
        self.__specificationsConstrucionTime = None
        self.__inputVector = None

        self.__parse_dspec(dspecFileName)
        self.__seriesProvider = SeriesProvider()

    def __parse_dspec(self, dspecFileName: str) -> None:
        """Parses the JSON Dspec file into a dspec object.
        """

        dspecFilePath = construct_true_path(getenv('DSPEC_FOLDER_PATH')) + dspecFileName

        if not path.exists(dspecFilePath):
            log(f'{dspecFilePath} not found!')
            raise FileNotFoundError
        
        with open(dspecFilePath) as dspecFile:
            
            #Meta and header data.
            json = load(dspecFile)

            dspec = Dspec()
            dspec.modelName = json["modelName"]
            dspec.modelVersion = json["modelVersion"]
            dspec.author = json["author"]
            dspec.modelFileName = json["modelFileName"]
            
            #OuputInfo
            outputJson = json["outputInfo"]
            
            outputInfo = OutputInfo()
            outputInfo.outputMethod = outputJson["outputMethod"]
            outputInfo.leadTime = outputJson["leadTime"]
            outputInfo.series = outputJson["series"]
            outputInfo.location = outputJson["location"]
            outputInfo.datum = outputJson["datum"]
            outputInfo.unit = outputJson["unit"]
            dspec.outputInfo = outputInfo #Bind to dspec

            #inputs
            inputsJson = json["inputs"]
            inputs = []
            
            for inputJson in inputsJson:
                input = Input()
                input.name = inputJson["name"]
                input.location = inputJson["location"]
                input.source = inputJson["source"]
                input.series = inputJson["series"]
                input.unit = inputJson["unit"]
                input.type = inputJson["type"]
                input.datum = inputJson.get("datum")
                input.interval = inputJson["interval"]
                input.range = inputJson["range"]
                inputs.append(input)
            dspec.inputs = inputs #Bind to dspec

            self.__dspec = dspec #Bind dspec to this obj

    def __generate_inputSpecifications(self, now: datetime) -> None:
        """Generates the list of input specifcations. This is a request paired
        with the expected datatype as a tuple. This list is saved as an atribute
        on this class. Also saves the time in which this was requested. This can be 
        used to determin if the specification needs to be remade.

        Parameters:
            now: datetime - The time to generate the sepcifications of of
            The data requests will be relative to this time.
        """
        specifications = []
        for input in self.__dspec.inputs:
            try:
                toDateTime = now + timedelta(hours= input.range[0])
                fromDateTime = now + timedelta(hours= input.range[1])

                #TODO:: Create better logic to propperly analyse a given input
                if (input.range[0] == input.range[1]): #isOnePoint
                    fromDateTime = fromDateTime.replace(minute=0, second=0, microsecond=0)

                #specifications is a pairing of a request and what type it should be
                specifications.append((
                    SeriesDescription(
                        input.source, 
                        input.series, 
                        input.location, 
                        input.unit, 
                        input.interval,
                        fromDateTime, 
                        toDateTime, 
                        input.datum
                    ),
                    input.type
                    )
                )
            except Exception as e:
                log(f'ERROR: There was a problem in the input generating inputrequests.\n\n Input= {input} Error= {e}')
        self.__specifications = specifications
        self.__specificationsConstrucionTime = now

    def __generate_inputVector(self) -> None:
        """This method fulfilles input specifications. IT queries the system
        for the data and casts the data acording to the dspec
        """
        inputVector = []
        for specification in self.__specifications:
            #unpack specification tuple
            requestDesc = specification[0]
            dataType = specification[1]

            responseSeries = self.__seriesProvider.make_request(requestDesc)
            if responseSeries.isComplete:
                [inputVector.append(dataPoint) for dataPoint in self.__cast_data(responseSeries.get_data(), dataType)]
            else:
                log(f'ERROR: There was a problem with input gathere making requests.\n\n Response: {responseSeries}\n\n')
        self.__inputVector = inputVector

    def __cast_data(self, data: list[Actual | Prediction], dataType: str) -> list[any]:
        """Casts vector of data to a given type.

        Parameters:
            data: list[Actual | Prediction] - The data to cast.
            dataTYpe: str = The datatype as a string to cast to.
        ReturnsL
            list[any] - The casted data
        """
        castedData = []
        #Cast from string to unit then append
        for datapoint in  data:
            match dataType:
                case 'float':
                    castedData.append(float(datapoint.value))
                case _:
                    log(f'Input gatherer has no conversion for Unit: {dataType}')
                    raise NotImplementedError
        return castedData

    def get_inputs(self, dateTime: datetime) -> list[any]:
        """Getter method returns the input vector for the loaded model. Only regenerates the vector
        if its a new request or a request with a different datetime.

        Parameters:
            dateTime: datetime - The datetime to base the input vector off of, the returned vector
            will be relative to it.
        """

        #Only regenerate specification IF its truely a new request.
        specificationIsCreated = self.__specifications != None
        requestTimeIsDifferent = dateTime != self.__specificationsConstrucionTime
        if not specificationIsCreated or (specificationIsCreated and requestTimeIsDifferent):
            self.__generate_inputSpecifications(dateTime)
            self.__generate_inputVector()
            return self.__inputVector
        else:
            return self.__inputVector

    def get_model_file_name(self) -> str:
        """Returns the name of the model as specified in the DSPEC file."""
        return self.__dspec.modelFileName     
    
    def get_dspec(self) -> Dspec:
        return self.__dspec

