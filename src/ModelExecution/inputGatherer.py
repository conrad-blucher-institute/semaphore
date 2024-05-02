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
from PostProcessing.IPostProcessing import post_processing_factory, post_process_data
#Libraries
from os import path, getenv
from json import load
from datetime import datetime, timedelta


class InputGatherer:
    """ The InputGatherer class which sends the dspc to be parsed, gets the dependant series
        checks for and calls any post processing and creates an ordered vector by referencing the keys
    """
    def __init__(self, dspecFileName: str) -> None:
        """Constructor sends the dspec file off to be loaded and parsed
        """
        self.__dspec = None
        self.__seriesDescriptionsTimeDescriptions = None
        self.__seriesConstructionTime = None
        self.__dependentSeriesSeries = None
        self.__postProcessedSeries = None
        self.__inputVector = None

        dspecFilePath = construct_true_path(getenv('DSPEC_FOLDER_PATH')) + dspecFileName

        if not path.exists(dspecFilePath):
            log(f'{dspecFilePath} not found!')
            raise FileNotFoundError

        parser = DSPEC_Parser(dspecFilePath)
        self.__dspec = parser.parse_dspec()
        self.__seriesProvider = SeriesProvider()

    def __generate_seriesDescription(self, referenceTime: datetime) -> None:
        """
        """
        # Create list of series descriptions and time descriptions from dependant series list
        seriesDescriptionsTimeDescriptions = []
        for series in self.__dspec.dependentSeries:
            try: 
                # Calculate the to and from time from the interval and range
                toDateTime = referenceTime + timedelta(seconds= input.range[0] * input.interval)
                fromDateTime = referenceTime + timedelta(seconds= input.range[1] * input.interval)
                
                # Check if it's only one point
                if (input.range[0] == input.range[1]): 
                    fromDateTime = fromDateTime.replace(minute=0, second=0, microsecond=0)
                
                # Create paring of series description and time description to pass to series provider
                seriesDescriptionsTimeDescriptions.append((
                    SeriesDescription(
                        series.source,
                        series.series, 
                        series.location, 
                        series.datum, 
                        series.verificationOverride
                    ), 
                    TimeDescription(
                        fromDateTime, 
                        toDateTime,
                        timedelta(seconds=series.interval)
                    )
                ))
            except Exception as e:
               log(f'ERROR: There was a problem in the input generating input requests.\n\n InputInfo= {input} Error= {e}')

        # Set the series description list and series construction time
        self.__seriesDescriptionsTimeDescriptions = seriesDescriptionsTimeDescriptions
        self.__seriesConstructionTime = referenceTime

    def __gather_data(self) -> None: 
        """
        """
        dependentSeriesSeries = []
        for dependentSeries in self.__seriesDescriptionsTimeDescriptions:
            # Unpack series and time description
            seriesDescription = dependentSeries[0]
            timeDescription = dependentSeries[1]

            # Get the data from series provider
            responseSeries = self.__seriesProvider.request_input(seriesDescription, timeDescription)

            # Check number of datapoints and if complete then append to list
            if responseSeries.isComplete:
                dependentSeriesSeries.append(responseSeries.data)
            else: 
                 log(f'ERROR: There was a problem with input gatherer making requests.\n\n Response: {responseSeries}\n\n')

        self.__dependentSeriesSeries = dependentSeriesSeries
        #there needs to be a dictionary thing going on here i think ^

    def __post_process_data(self) -> None:
        """
        """
        pass

    def __order_input_vector(self) -> None:
        """
        """
        pass


    
    def get_inputs(self, dateTime: datetime) -> list[any]:
        """Getter method returns the input vector for the loaded model. Only regenerates the vector
        if its a new request or a request with a different datetime.

        Parameters:
            dateTime: datetime - The datetime to base the input vector off of, the returned vector
            will be relative to it.
        """
        # Only go about creating input vector if the request is new
        seriesCreated = self.__seriesDescriptionsTimeDescriptions != None
        requestTimeIsDifferent = dateTime != self.__seriesConstructionTime
        if not seriesCreated or (seriesCreated and requestTimeIsDifferent):
            # The dspec was parsed in the constructor
            # Convert dependent series described in the dspec into series description objects
            self.__generate_seriesDescription(dateTime)

            # Call series provider to gather inputs
            self.__gather_data()

            # Call post processing 
            self.__post_process_data()

            # Order series according to keys and cast data
            self.__order_input_vector()

            #Return ordered input vector
            return self.__inputVector
        # Otherwise just return current input vector
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