# -*- coding: utf-8 -*-
#DataClasses.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/30/2023
# version 2.0
#----------------------------------
"""This file holds for classes that the system will use to describes series of data, actuals, and predictions.
 """ 
#----------------------------------
# 
#
#Imports
from datetime import datetime
from typing import List


class Actual():
    """An Actual is a measurement of some environment variable can be linked to a date time.
        :param value: str - The actual measurements value
        :param unit: str - the type the value is of
        :param dateTime: datetime - the time the measurement was taken
        :param longitude: str = None
        :param latitude: str = None
    """
    def __init__(self, value: str, unit: str, dateTime: datetime, longitude: str = None, latitude: str = None) -> None:
        self.value = value
        self.unit = unit
        self.dateTime = dateTime
        self.longitude = longitude
        self.latitude = latitude

    def __str__(self) -> str:
        return f'\n[Actual] -> value: {self.value}, unit: {self.unit}, dataTime: {self.dateTime}. longitude: {self.longitude}. latitude: {self.latitude}'
    
    def __repr__(self) -> str:
        return f'\n[Actual] -> value: {self.value}, unit: {self.unit}, dataTime: {self.dateTime}. longitude: {self.longitude}. latitude: {self.latitude}'
    
    def __eq__(self, __value: object) -> bool:
        if (self.value == __value.value and 
            self.unit == __value.unit and 
            self.dateTime == __value.dateTime and 
            self.longitude == __value.longitude and 
            self.latitude == __value.latitude):
            return True
        else: return False

class Prediction():
    """A prediction is a Is a predicted value. It has all the same things as an Actual, but it describes its time as the time the prediction
    was made and the lead time till its verification time.
    generatedTime + leadTime = verificationTime
        :param value: str - The actual measurements value
        :param unit: str - the type the value is of
        :param leadTime: float - The time in hours between the generated time and verification time
        :param generatedTime: datetime - Time the prediction was created
        :param successValue: str - Planned but not yet used. Its a place to save a confidence value for example.
        :param longitude = : str = None
        :param latitude = : str = None
    """
    def __init__(self, value: str, unit: str, leadTime: float, generatedTime: datetime, successValue: str = None, longitude: str = None, latitude: str = None) -> None:
        self.value = value
        self.unit = unit
        self.leadTime = leadTime
        self.generatedTime = generatedTime
        self.successValue = successValue #Planned but not yet used. Its a place to save a confidence value for example.
        self.longitude = longitude
        self.latitude = latitude
    
    def __str__(self) -> str:
        return f'\n[Prediction] -> value: {self.value}, unit: {self.unit}, leadTime: {self.leadTime}, generatedTime: {self.generatedTime}, successValue {self.successValue}. longitude: {self.longitude}. latitude: {self.latitude}'
    
    def __repr__(self):
        return f'\n[Prediction] -> value: {self.value}, unit: {self.unit}, leadTime: {self.leadTime}, generatedTime: {self.generatedTime}, successValue {self.successValue}. longitude: {self.longitude}. latitude: {self.latitude}'
    
    def __eq__(self, __value: object) -> bool:
        if (self.value == __value.value and 
            self.unit == __value.unit and 
            self.leadTime == __value.leadTime and 
            self.generatedTime == __value.generatedTime and 
            self.successValue == __value.successValue and 
            self.longitude == __value.longitude and 
            self.latitude == __value.latitude):
            return True
        else: return False

    

class SeriesDescription():
    """A series description should describe a set of data without actually including the data.
        :param source: str - The data's source Ex. NOAA tides and currents.
        :param series: str - The series name
        :param dataClassification: str - actual/prediction/output identifier 
        :param location: str - The location of the data
        :param unit: str - The unit (Ex. meter, foot)
        :param interval: int - The time step separating the datapoints in order
        :param fromDateTime: datetime - The datetime the data starts at
        :param toDateTime: datetime - The datetime the data stops at
        :param datum: str = None
    """
    def __init__(self, source: str, series: str, dataClassification: str, location: str, unit: str, interval: int, fromDateTime: datetime, toDateTime: datetime, datum: str = None) -> None:
        self.source = source
        self.series = series
        self.dataClassification = dataClassification
        self.location = location
        self.unit = unit
        self.datum = datum
        self.interval = interval
        self.fromDateTime = fromDateTime
        self.toDateTime = toDateTime

    def __str__(self) -> str:
        return f'\n[SeriesDescription] -> source: {self.source}, series: {self.series}, dataClassification: {self.dataClassification}, location: {self.location}, unit: {self.unit}, datum {self.datum}, interval: {self.interval}, fromDateTime {self.fromDateTime}, toDateTime {self.toDateTime}'
    
    def __repr__(self) -> str:
        return f'\n[SeriesDescription] -> source: {self.source}, series: {self.series}, dataClassification: {self.dataClassification}, location: {self.location}, unit: {self.unit}, datum {self.datum}, interval: {self.interval}, fromDateTime {self.fromDateTime}, toDateTime {self.toDateTime}'

class SemaphoreSeriesDescription():
    """A semaphore series description should describe a set of predictions that is actually generated by semaphore.
        :param ModelName: str - The name of the model
        :param ModelVersion: str - The version of the model
        :param series: str - The series name
        :param location: str - The data's location
        :param datum: str = None
    """
    def __init__(self, ModelName: str, ModelVersion: str, series:str, location: str, datum: str = None) -> None:
        self.ModelName = ModelName
        self.ModelVersion = ModelVersion
        self.series = series
        self.location = location
        self.datum = datum

    def __str__(self) -> str:
        return f'\n[SemaphoreSeriesDescription] -> AIName: {self.ModelName}, AIGeneratedVersion: {self.ModelVersion}, location: {self.location}, datum: {self.datum}'
    
    def __repr__(self) -> str:
        return f'\n[SemaphoreSeriesDescription] -> AIName: {self.ModelName}, AIGeneratedVersion: {self.ModelVersion}, location: {self.location}, datum: {self.datum}'

class Series():
    """A series is a pairing of an object describing a series of data, and an array of the data itself. It also includes some meta data about how well the data provided matches the description.
        :param description: SeriesDescription | SemaphoreSeriesDescription - An object that will describe to you what unique data set this is.
        :param isComplete: bool - Weather the description object completely describes the bound data.
        :param nonCompleteReason: Exception = None - The exception that cased the problem
    """
    def __init__(self, description: SeriesDescription | SemaphoreSeriesDescription, isComplete: bool, nonCompleteReason: Exception = None) -> None:
        self.description = description
        self.isComplete = isComplete
        self.nonCompleteReason = nonCompleteReason
        self.__data = []

    @property
    def data(self):
        return self.__data
    
    @data.setter
    def data(self, data: List[Actual | Prediction]) -> None:
        self.__data = data

    def __str__(self) -> str:
        return f'\n[Series] -> description: {self.description}, wasSuccessful: {self.isComplete}, errorReason: {self.nonCompleteReason}'
    
