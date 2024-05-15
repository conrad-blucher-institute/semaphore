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
from DataClasses import SeriesDescription, TimeDescription, Input, DataIntegrityDescription
from .dspecParser import DSPEC_Parser, Dspec, DependentSeries
from utility import log, construct_true_path
from PostProcessing.IPostProcessing import post_processing_factory

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
        self.__inputSeriesDict = None
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
        This function generates the series description for each dependentSeries input

        param: refrenceTime - datetime : Used to calculate the to and from time 
         for timeDescripion
        """
        # Create list of series descriptions and time descriptions from dependant series list
        seriesDescriptionsTimeDescriptions = []
        for series in self.__dspec.dependentSeries:
            try: 
                # Calculate the to and from time from the interval and range
                toDateTime = referenceTime + timedelta(seconds= series.range[0] * series.interval)
                fromDateTime = referenceTime + timedelta(seconds= series.range[1] * series.interval)
                
                # Check if it's only one point
                if (series.range[0] == series.range[1]): 
                    fromDateTime = fromDateTime.replace(minute=0, second=0, microsecond=0)
                
                # Create pairing of series description and time description to pass to series provider
                seriesDescriptionsTimeDescriptions.append((
                    SeriesDescription(
                        series.source,
                        series.series, 
                        series.location, 
                        series.datum, 
                        self.__get_dataIntegrityCall(series),
                        series.verificationOverride
                    ), 
                    TimeDescription(
                        fromDateTime, 
                        toDateTime,
                        timedelta(seconds=series.interval)
                    ), 
                    series.outKey
                ))
            except Exception as e:
               log(f'ERROR: There was a problem in the input generating input requests.\n\n InputInfo= {series} Error= {e}')

        # Set the series description list and series construction time
        self.__seriesDescriptionsTimeDescriptions = seriesDescriptionsTimeDescriptions
        self.__seriesConstructionTime = referenceTime

    def __get_dataIntegrityCall(self, dependentSeries: DependentSeries) -> DataIntegrityDescription:
        """This method should return None if there is no Data Integrity Call, 
        else it makes a DataIntegrityDescription and returns that"""
        if dependentSeries.dataIntegrityCall is None: return None
        else: return DataIntegrityDescription(
                            dependentSeries.dataIntegrityCall.call,
                            dependentSeries.dataIntegrityCall.args
                    )



    def __gather_data(self) -> None: 
        """
        This function takes calls the series providor for each series created from the
        dependentSeries list in the dspec and adds the data and it's key to the inputSeriesDict
        """
        dependentSeriesSeries = {}
        for dependentSeries in self.__seriesDescriptionsTimeDescriptions:
            # Unpack series and time description
            seriesDescription = dependentSeries[0]
            timeDescription = dependentSeries[1]
            key = dependentSeries[2]

            # Get the data from series provider
            responseSeries = self.__seriesProvider.request_input(seriesDescription, timeDescription)

            # Check number of datapoints and if complete then append to list
            if responseSeries.isComplete:
                dependentSeriesSeries[key] = responseSeries
            else: 
                raise RuntimeError(f'ERROR: There was a problem with input gatherer making requests.\n\nResponse: \t{responseSeries}\n\n')

        self.__inputSeriesDict = dependentSeriesSeries

    def __post_process_data(self) -> None:
        """
        This function calls the post processing methods for any inputs that need post processing
        the post_process_data function is passed the input dictionary and the process call
        so that the function can easily find the series needed for the computation and return 
        a dictionary with the new outkeys and series. 
        """
        for postProcess in self.__dspec.postProcessCall:
            # Instantiate Factory Method
            processing_Class = post_processing_factory(postProcess.call)
            # Call Post Processing Function
            log(f'Init Post processing: \n\t{postProcess.call}\n\tArgs: {postProcess.args}')
            newProcessedInput = processing_Class.post_process_data(self.__inputSeriesDict, postProcess)
            # Add preprocessed dict to the inputSeries dict
            self.__inputSeriesDict.update(newProcessedInput)

    def __order_input_vector(self) -> None:
        """
        This function sorts through the inputSeriesDict and adds any series with matching
        outkeys to the input_vector in the correct order with the data cast to the correct 
        data type. 
        """
        target_keys = self.__dspec.orderedVector.keys
        data_types = self.__dspec.orderedVector.dTypes
        input_vector = []

        log('Ordered Vector:')
        for key, dtype in zip(target_keys, data_types):            

            # Checking for each key in the input series dictionary 
            if key in self.__inputSeriesDict:
                casted_data = self.__cast_data(self.__inputSeriesDict[key].data, dtype)
                log(f'\t{key}: - amnt_found: {len(casted_data)}')
                input_vector += casted_data
            else:
                log(f'ERROR: There was a problem with input gatherer finding outKey {key} in {self.__inputSeriesDict}')

        self.__inputVector = input_vector

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
        
    def calculate_referenceTime(self, execution :datetime) -> datetime:
        '''This function calculates the refrence time that semaphore needs to use to get the correct number of inputs from execution time
        :param execution: datetime -the execution time'''

        interval = self.__dspec.timingInfo.interval

        referenceTime = datetime.utcfromtimestamp(execution.timestamp() - (execution.timestamp() % interval))

        return referenceTime
            
    def get_model_file_name(self) -> str:
        """Returns the name of the model as specified in the DSPEC file."""
        return self.__dspec.modelFileName     
    
    def get_dspec(self) -> Dspec:
        """Returns the dspec object being used."""
        return self.__dspec

    def get_input_specifications(self) -> list:
        """Returns list of input specifications."""
        return self.__specifications