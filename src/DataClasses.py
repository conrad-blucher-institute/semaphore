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
from datetime import datetime
from typing import List

class Input():
    """An Input is a data value of some environment variable that can be linked to a date time.
        :param value: str - The actual data value
        :param unit: str - The unit of measurement of the value
        :param timeGenerated: datetime - The datetime that the value was created
        :param timeAcquired: datetime - The datetime that the value was acquired by Semaphore
        :param timeVerified: datetime - The datetime that the value is valid
        :param longitude: str = None
        :param latitude: str = None
    """
    def __init__(self, value: str, unit: str, timeAcquired: datetime, timeVerified: datetime, timeGenerated: datetime = None, longitude: str = None, latitude: str = None) -> None:
        self.value = value
        self.unit = unit
        self.timeAcquired = timeAcquired,
        self.timeVerified = timeVerified,
        self.timeGenerated = timeGenerated,
        self.longitude = longitude
        self.latitude = latitude

    def __str__(self) -> str:
        return f'\n[Input] -> value: {self.value}, unit: {self.unit}, timeGenerated: {self.timeGenerated}, timeAcquired: {self.timeAcquired}, \
            timeVerified: {self.timeVerified}. longitude: {self.longitude}. latitude: {self.latitude}'
    
    def __repr__(self) -> str:
        return f'\nInput({self.value}, {self.unit}, {self.timeGenerated}, {self.timeAcquired}, {self.timeVerified}, {self.longitude}, {self.latitude})'
    
    def __eq__(self, __value: object) -> bool:
        if (self.value == __value.value and 
            self.unit == __value.unit and 
            self.timeGenerated == __value.timeGenerated and
            self.timeAcquired == __value.timeAcquired and
            self.timeVerified == __value.timeVerified and
            self.longitude == __value.longitude and 
            self.latitude == __value.latitude):
            return True
        else: return False


class Output():
    """An output is a a predicted value created by the model semaphore is running. It has all the same things as an Actual, but it describes its time as the time the prediction
    was made and the lead time till its verification time.
    generatedTime + leadTime = verificationTime
        :param value: str - The actual measurements value
        :param unit: str - the type the value is of
        :param leadTime: float - The time in hours between the generated time and verification time
        :param generatedTime: datetime - Time the prediction was created
    """
    def __init__(self, value: str, unit: str, leadTime: float, generatedTime: datetime) -> None:
        self.value = value
        self.unit = unit
        self.leadTime = leadTime
        self.generatedTime = generatedTime
    
    def __str__(self) -> str:
        return f'\n[Prediction] -> value: {self.value}, unit: {self.unit}, leadTime: {self.leadTime}, generatedTime: {self.generatedTime}'
    
    def __repr__(self):
        return f'\n[Prediction] -> value: {self.value}, unit: {self.unit}, leadTime: {self.leadTime}, generatedTime: {self.generatedTime}'
    
    def __eq__(self, __value: object) -> bool:
        if (self.value == __value.value and 
            self.unit == __value.unit and 
            self.leadTime == __value.leadTime and 
            self.generatedTime == __value.generatedTime):
            return True
        else: return False


class TimeDescription():
    """A time description should describe the datetime properties of a dataset.
        :param fromDateTime: datetime - The datetime the data starts at
        :param toDateTime: datetime - The datetime the data stops at
        :param interval: int = None - The time step separating the data points in order
    """
    def __init__(self, fromDateTime: datetime, toDateTime: datetime, interval: int = None) -> None:
        self.fromDateTime = fromDateTime,
        self.toDateTime = toDateTime,
        self.interval = interval

    def __str__(self) -> str:
        return f'\n[TimeDescription] -> fromDateTime: {self.fromDateTime}, toDateTime: {self.toDateTime}. interval: {self.interval}'
    
    def __repr__(self) -> str:
        return f'\nTimeDescription({self.fromDateTime}, {self.toDateTime}, {self.interval})'


class SeriesDescription():
    """A series description should describe a set of data without actually including the data.
        :param source: str - The data's source (e.g. 'NOAATANDC')
        :param series: str - The series name (e.g. 'x_wind')
        :param location: str - The location of the data ('packChan')
        :param datum: str = None
    """
    def __init__(self, source: str, series: str, location: str, datum: str = None) -> None:
        self.source = source
        self.series = series
        self.location = location
        self.datum = datum

    def __str__(self) -> str:
        return f'\n[SeriesDescription] -> source: {self.source}, series: {self.series}, location: {self.location}. datum {self.datum}'
    
    def __repr__(self) -> str:
        return f'\nSeriesDescription({self.source}, {self.series}, {self.location}, {self.datum})'


class SemaphoreSeriesDescription():
    """A semaphore series description should describe a set of predictions that is actually generated by semaphore.
        :param ModelName: str - The name of the model
        :param ModelVersion: str - The version of the model
        :param series: str - The series name
        :param location: str - The data's location
        :param interval: int - The time step separating the datapoints in order
        :param fromDateTime: datetime - The datetime the data starts at
        :param toDateTime: datetime - The datetime the data stops at
        :param datum: str = None
        :param leadTime: float - The time in hours between the generated time and verification time
    """
    def __init__(self, ModelName: str, ModelVersion: str, series:str, location: str, interval: int, leadTime: float, datum: str = None) -> None:
        self.ModelName = ModelName
        self.ModelVersion = ModelVersion
        self.series = series
        self.location = location
        self.interval = interval
        self.fromDateTime = None
        self.toDateTime = None
        self.leadTime = leadTime
        self.datum = datum

    def __str__(self) -> str:
        return f'\n[SemaphoreSeriesDescription] -> AIName: {self.ModelName}, AIGeneratedVersion: {self.ModelVersion}, location: {self.location}, interval: {self.interval}, fromDateTime {self.fromDateTime}, toDateTime {self.toDateTime}, leadTime {self.leadTime}, datum: {self.datum}'
    
    def __repr__(self) -> str:
        return f'\n[SemaphoreSeriesDescription] -> AIName: {self.ModelName}, AIGeneratedVersion: {self.ModelVersion}, location: {self.location}, interval: {self.interval}, fromDateTime {self.fromDateTime}, toDateTime {self.toDateTime}, leadTime {self.leadTime}, datum: {self.datum}'


class Series():
    """A series is a pairing of an object describing a series of data, and an array of the data itself. It also includes some meta data about how well the data provided matches the description.
        :param description: SeriesDescription | SemaphoreSeriesDescription - An object that will describe to you what unique data set this is
        :param isComplete: bool - Whether the description object completely describes the bound data
        :param timeDescription: TimeDescription = None - An object that contains the datetime properties of the data
        :param nonCompleteReason: Exception = None - The exception that cased the problem
    """
    def __init__(self, description: SeriesDescription | SemaphoreSeriesDescription, isComplete: bool, timeDescription: TimeDescription = None, nonCompleteReason: Exception = None) -> None:
        self.description = description
        self.isComplete = isComplete
        self.timeDescription = timeDescription
        self.nonCompleteReason = nonCompleteReason
        self.__data = []

    @property
    def data(self):
        return self.__data
    
    @data.setter
    def data(self, data: List[Input | Output]) -> None:
        self.__data = data

    def __str__(self) -> str:
        return f'\n[Series] -> description: {self.description}, wasSuccessful: {self.isComplete}, timeDescription: {self.timeDescription}, errorReason: {self.nonCompleteReason}'
    