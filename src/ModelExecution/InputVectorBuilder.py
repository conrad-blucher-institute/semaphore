# -*- coding: utf-8 -*-
#inputVectorBuilder.py
#----------------------------------
# Created By: Matthew Kastl
# Version: 1.0
#----------------------------------
""" 
This file is responsible for generating batches of input vectors. It is responsible for:
    - Generating one or more input vectors required by the specification
    - Casting the data to the correct type
    - Selecting the correct indexing of the data for the input vector
    - Iterate over multi value inputs to generate a batch of vectors
 """ 
#----------------------------------
# 
#
#Imports
from DataClasses import Series
from .dspecParser import VectorOrder, Dspec
from utility import log
from exceptions import Semaphore_Exception
from typing import Generator

class InputVectorBuilder:

    def build_batch(self, dspec: Dspec, dataRepository: dict[str, Series]) -> list[list[any]]:
        """ This is the main public method for this class. It will build a batch of input vectors as specified by the dspec.
        It will:
            - Read the vector order object from the dspec
            - Build one or more input vectors
                - Cast the data to the correct type
                - Index the requested data
                - Concatenate the data into an input vector
                - Select the correct data from multi series
            - Check the correct amount of input vectors was created
        NOTE:: This could be one or more input vectors depending on the specification.
        NOTE:: It will check the batch length against the amount of Expected Vectors if it was provided. (1 if not provided)

        :param dspec: Dspec - The dspec object to read from
        :param dataRepository: dict[str, Series] - The repository of all the data from input gatherer
        :returns : list[list[any]] - The batch of input vectors
        :warns - If the batch length is not equal to the amount of expected vectors
        """

        # Parse VO
        vo: VectorOrder = dspec.orderedVector
        amntExpectedVectors = vo.amntExpectedVectors

        # If no amntExpectedVectors assume 1
        if amntExpectedVectors is None:
            amntExpectedVectors = 1

        # Build the batch
        batch = []
        input_vector_generator = self.__buildInputVector(dataRepository)
        while True:
            input_vector = next(input_vector_generator)
            if input_vector is None: break
            else: batch.append(input_vector)

        # Check the batch is of the expected length
        if len(batch) != amntExpectedVectors:
            log(f'Warning: build_input_batch {amntExpectedVectors} vectors but only found {len(batch)}')

        return batch

    def __buildInputVector(self, vectorOrder: VectorOrder, dataRepository: dict[str, Series]) -> Generator[list[any], None, None]:
        """ Builds a input vector based on the vector order specification.
        The function can be given a mix of series with single value inputs or multi value inputs. For series marked multi
        it will return the next index of the value each time the generator is called. It returns None when every possible vector has 
        been generated.
        
        :param dataRepository: dict[str, Series] - The repository of all the data from input gatherer
        :returns: Generator[list[any], None, None]
            :yeild list[any] - The input vector
            :returns None - When every possible vector has been generated
            :raises Semaphore_Exception - If a series is missing from the data repository
            :warns - If a series is marked multi but only has one value per input
        """
        batchIndex = 0 # Keeps track of what index vector we are on, to index multi inputs
        ordered_keys = vectorOrder.keys
        ordered_dtypes = vectorOrder.dTypes
        ordered_indexes = vectorOrder.indexes
        multipliedKeys = vectorOrder.multipliedKeys
        
        
        isFinished = False

        while True:         
            if isFinished: 
                return None # Every possible vector has been generated
            
            input_vector = []
            # We iterate over every series the input vector has in order
            for key, dtype, index in zip(ordered_keys, ordered_dtypes, ordered_indexes):            
                
                log(f'\tVector batch index: {batchIndex} _______________________________________')

                # Check to see if this series is marked as a multi input series
                keyIsMulti = key in multipliedKeys 

                # Get the Series from the data repository, error if its missing!
                series = dataRepository.get(key)
                if series is None:
                    raise Semaphore_Exception(f'ERROR: There was a problem with input gatherer finding outKey {key} in {dataRepository}')
                
                # Grab all data, this changes if its a multi series or not
                data = None
                isFinished = True # Assume we are finished unless we find a multi series that has more data
                if keyIsMulti:
                    lengthOfMultiSeries = len(series.data[0].dataValue)
                    if lengthOfMultiSeries <= batchIndex: 
                        # We want the data for just this batch
                        data = [input.dataValue[batchIndex] for input in series.data]
                        isFinished = False # There could be more vectors to make
                    else:
                        if batchIndex == 1: log("Warning:: build input vectors returning with a batch index of 1. This indicates a series was marked multi but only actually included one value per input!")
                        isFinished = True # All possible vectors have been generated
                else:
                    data = [input.dataValue for input in series.data]

                # Cast Data
                casted_data = [self.__cast_value(d, dtype) for d in data]

                # Select only the wanted data
                indexed_data = casted_data[index[0] : index[1]]
                
                log(f'\t\t{key}: - amnt_found: {len(casted_data)}, indexed_len: {len(indexed_data)}')
                # Concatenate the designated slice of casted data into the input vector
                input_vector += indexed_data

            batchIndex += 1   
            yield input_vector
    

    def __cast_value(self, value: any, dataType: str) -> any:
        """This function casts the value to the correct type based on the data type specified in the vector order.
        :param value: any - The value to cast
        :param dataType: str - The data type to cast the value to
        :returns: any - The casted value
        """
        match dataType:
            case 'float':
                return float(value)
            case _:
                log(f'Input gatherer has no conversion for Unit: {dataType}')
                raise NotImplementedError


