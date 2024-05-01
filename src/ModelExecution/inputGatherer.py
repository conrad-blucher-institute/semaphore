# -*- coding: utf-8 -*-
#inputGatherer.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 2/3/2023
# Version: 2.0
#----------------------------------
""" This file communicates with series provider to request series and build the input vector for a model.
    The input gatherer also checks for post processing steps and instantiates each class in the right 
    order from the dspec to post process the data collected. 
 """ 
#----------------------------------
# 
#
#Imports
#Local
from SeriesProvider.SeriesProvider import SeriesProvider
from DataClasses import SeriesDescription, TimeDescription, Input
from .dspecParser import DSPEC_Parser, Dspec
from utility import log, construct_true_path
#Libraries
from os import path, getenv
from json import load
from datetime import datetime, timedelta


class InputGatherer:
    """ The InputGatherer class ...
    """
    def __init__(self, dspecFileName: str) -> None:
        """Constructor sends the dspec file off to be loaded and parsed
        """
        self.__dspec = None
        self.__specifications = None
        self.__specificationsConstructionTime = None
        self.__inputVector = None

        dspecFilePath = construct_true_path(getenv('DSPEC_FOLDER_PATH')) + dspecFileName

        if not path.exists(dspecFilePath):
            log(f'{dspecFilePath} not found!')
            raise FileNotFoundError

        parser = DSPEC_Parser(dspecFilePath)
        self.__dspec = parser.parse_dspec()
        self.__seriesProvider = SeriesProvider()

   # -> Parse dspec -> get dependent series -> calls any post processors -> create the ordered vector by referencing the keys


    def __generate_inputSpecifications(self, referenceTime: datetime) -> None:
        """Generates the list of input specifications. This is a request paired
        with the expected datatype as a list. This list is saved as an attribute
        on this class. Also saves the time in which this was requested. This can be 
        used to determine if the specification needs to be remade.

        Parameters:
            referenceTime: datetime - The time to generate the specification of
            The data requests will be relative to this time.
        """
        specifications = []
        for input in self.__dspec.inputs:
            try:
                #Calculate the to and from time from the interval and range
                toDateTime = referenceTime + timedelta(seconds= input.range[0] * input.interval)
                fromDateTime = referenceTime + timedelta(seconds= input.range[1] * input.interval)
                
                #TODO:: Create better logic to properly analyse a given input
                if (input.range[0] == input.range[1]): #isOnePoint
                    fromDateTime = fromDateTime.replace(minute=0, second=0, microsecond=0)

                #specifications is a pairing of a request and what type it should 
                specifications.append((
                    SeriesDescription(
                        input.source, 
                        input.series,
                        input.location,
                        input.datum,
                        input.verificationOverride
                    ),
                    TimeDescription(
                        fromDateTime,
                        toDateTime,
                        timedelta(seconds=input.interval)
                    ),
                    input.type
                    )
                )
            except Exception as e:
               log(f'ERROR: There was a problem in the input generating input requests.\n\n InputInfo= {input} Error= {e}')
        self.__specifications = specifications
        self.__specificationsConstructionTime = referenceTime

    def __generate_inputVector(self) -> None:
        """This method fills input specifications. It queries the system
        for the data and casts the data according to the dspec
        """
        inputVector = []
        for specification in self.__specifications:
            #unpack specification
            seriesDescription = specification[0]
            timeDescription = specification[1]
            dataType = specification[2]

            responseSeries = self.__seriesProvider.request_input(seriesDescription, timeDescription)
            if responseSeries.isComplete:
                [inputVector.append(dataPoint) for dataPoint in self.__cast_data(responseSeries.data, dataType)]
            else:
                log(f'ERROR: There was a problem with input gatherer making requests.\n\n Response: {responseSeries}\n\n')
        self.__inputVector = inputVector

    def __cast_data(self, data: list[Input], dataType: str) -> list[any]:
        """Casts vector of data to a given type.

        Parameters:
            data: list[Input] - The data to cast.
            dataType: str = The datatype as a string to cast to.
        ReturnsL
            list[any] - The casted data
        """
        castedData = []
        #Cast from string to unit then append
        for datapoint in data:
            match dataType:
                case 'float':
                    castedData.append(float(datapoint.dataValue))
                case _:
                    log(f'Input gatherer has no conversion for Unit: {dataType}')
                    raise NotImplementedError
        return castedData
    
    def calculate_referenceTime(self, execution :datetime) -> datetime:
        '''This function calculates the refrence time that semaphore needs to use to get the correct number of inputs from execution time
        :param execution: datetime -the execution time'''

        interval = self.__dspec.timingInfo.interval

        referenceTime = datetime.utcfromtimestamp(execution.timestamp() - (execution.timestamp() % interval))

        return referenceTime
    
    def get_inputs(self, dateTime: datetime) -> list[any]:
        """Getter method returns the input vector for the loaded model. Only regenerates the vector
        if its a new request or a request with a different datetime.

        Parameters:
            dateTime: datetime - The datetime to base the input vector off of, the returned vector
            will be relative to it.
        """
        
        #Only regenerate specification If its truly a new request.
        specificationIsCreated = self.__specifications != None
        requestTimeIsDifferent = dateTime != self.__specificationsConstructionTime
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

    def get_input_specifications(self) -> list:
        """Returns list of input specifications."""
        return self.__specifications