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
from datetime import datetime, timedelta
from typing import List

class Input():
    """An Input is a data value of some environment variable that can be linked to a date time.
        :param value: str - The actual data value
        :param unit: str - The unit of measurement of the value
        :param timeAcquired: datetime - The datetime that the value was acquired by Semaphore
        :param timeVerified: datetime - The datetime that the value is valid
        :param timeGenerated: datetime = None - The datetime that the value was created
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
    """An output is a a predicted value created by the model semaphore is running.
        :param value: str - The actual data value
        :param unit: str - The unit of measurement of the value
        :param timeGenerated: datetime - The datetime that the value was created
        :param longitude: str = None
        :param latitude: str = None
    """
    def __init__(self, value: str, unit: str, timeGenerated: datetime, longitude: str = None, latitude: str = None) -> None:
        self.value = value
        self.unit = unit
        self.timeGenerated = timeGenerated
        self.latitude = latitude
        self.longitude = longitude
        
    def __str__(self) -> str:
        return f'\n[Output] -> value: {self.value}, unit: {self.unit}, timeGenerated: {self.timeGenerated}, longitude: {self.longitude},latitude: {self.latitude}'
    
    def __repr__(self):
        return f'\nOutput({self.value},{self.unit},{self.timeGenerated},{self.longitude},{self.latitude})'
    
    def __eq__(self, __value: object) -> bool:
        if (self.value == __value.value and 
            self.unit == __value.unit and  
            self.timeGenerated == __value.timeGenerated and
            self.longitude == __value.longitude and 
            self.latitude == __value.latitude):
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
        :param location: str - The data's location
        :param leadTime: timedelta - The time in hours between the generated time and verification time
        :param datum: str = None
    """
    def __init__(self, ModelName: str, ModelVersion: str, location: str, leadTime: timedelta, datum: str = None) -> None:
        self.ModelName = ModelName
        self.ModelVersion = ModelVersion
        self.location = location
        self.leadTime = leadTime
        self.datum = datum

    def __str__(self) -> str:
        return f'\n[SemaphoreSeriesDescription] -> AIName: {self.ModelName}, AIGeneratedVersion: {self.ModelVersion}, location: {self.location}, leadTime {self.leadTime}, datum: {self.datum}'
    
    def __repr__(self) -> str:
        return f'\nSemaphoreSeriesDescription({self.ModelName},{self.ModelVersion}, {self.location},{self.leadTime},{self.datum})'


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
    