# -*- coding: utf-8 -*-
#DataClasses.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/30/2023
# version 2.0
#----------------------------------
"""This file holds for classes that the system will use to describes inputs and outputs.
 """ 
#----------------------------------
# 
#
from datetime import datetime, timedelta, time
from typing import List

class Input():
    """An Input is a data value of some environment variable that can be linked to a date time.
        :param dataValue: str - The actual data value
        :param dataUnit: str - The unit of measurement of the value
        :param timeVerified: datetime 
        :param timeGenerated: datetime
        :param longitude: str = None
        :param latitude: str = None

    """
    def __init__(self, dataValue: str, dataUnit: str, timeVerified: datetime, timeGenerated: datetime,  longitude: str = None, latitude: str = None) -> None:
        self.dataValue = dataValue
        self.dataUnit = dataUnit
        self.timeVerified = timeVerified
        self.timeGenerated = timeGenerated
        self.longitude = longitude
        self.latitude = latitude
        

    def __str__(self) -> str:
        return f'\n[Input] -> value: {self.dataValue}, unit: {self.dataUnit},timeVerified:{self.timeVerified}, timeGenerated:{self.timeGenerated}, longitude: {self.longitude}. latitude: {self.latitude}'
    
    def __repr__(self) -> str:
        return f'\nInput({self.dataValue}, {self.dataUnit}, {self.timeVerified},{self.timeGenerated}, {self.longitude}, {self.latitude})'
    
    def __eq__(self, __value: object) -> bool:
        if (self.dataValue == __value.dataValue and 
            self.dataUnit == __value.dataUnit and 
            self.timeVerified == __value.timeVerified and
            self.timeGenerated == __value.timeGenerated and
            self.longitude == __value.longitude and 
            self.latitude == __value.latitude):
            return True
        else: return False


class Output():
    """An output is a a predicted value created by the model semaphore is running.
        :param dataValue: str - The actual data value
        :param dataUnit: str - The unit of measurement of the value
        :param timeGenerated: datetime - The datetime that the value was created
        :parm leadTime: timedelta - The lead time for the model
    """
    def __init__(self, dataValue: str, dataUnit: str, timeGenerated: datetime, leadTime: timedelta) -> None:
        self.dataValue = dataValue
        self.dataUnit = dataUnit
        self.timeGenerated = timeGenerated
        self.leadTime = leadTime
        
    def __str__(self) -> str:
        return f'\n[Output] -> Value: {self.dataValue}, Unit: {self.dataUnit}, TimeGenerated: {self.timeGenerated}, LeadTime: {self.leadTime}'
    
    def __repr__(self):
        return f'\nOutput({self.dataValue}, {self.dataUnit}, {self.timeGenerated}, {self.leadTime})'
    
    def __eq__(self, __value: object) -> bool:
        if (self.dataValue == __value.dataValue and 
            self.dataUnit == __value.dataUnit and  
            self.timeGenerated == __value.timeGenerated and
            self.leadTime == __value.leadTime):
            return True
        else: return False

class DataIntegrityDescription():
    """A time description should describe the data Integrity steps that should be taken
        :param call: str - The Data Integrity class to call.
        :param args: dict[str, str] - The argument dictionary
    """

    def __init__(self, call: str, args: dict[str, str]):
        self.call = call
        self.args = args

    def __str__(self) -> str:
        return f'\n[DataIntegrityDescription] -> call: {self.call}, args: {self.args}'
    
    def __repr__(self) -> str:
        return f'\nDataIntegrityDescription({self.call}, {self.args})'

class TimeDescription():
    """A time description should describe the datetime properties of a dataset.
        :param fromDateTime: datetime - The datetime the data starts at
        :param toDateTime: datetime - The datetime the data stops at
        :param interval: timedelta = None - The time step separating the data points in order
        :parm leadTime: timedelta - The lead time for the model
    """
    def __init__(self, fromDateTime: datetime, toDateTime: datetime, interval: timedelta = None) -> None:
        self.fromDateTime = fromDateTime
        self.toDateTime = toDateTime
        self.interval = interval
        

    def __str__(self) -> str:
        return f'\n[TimeDescription] -> fromDateTime: {self.fromDateTime}, toDateTime: {self.toDateTime}, interval: {self.interval}'
    
    def __repr__(self) -> str:
        return f'\nTimeDescription({self.fromDateTime}, {self.toDateTime}, {self.interval})'


class SeriesDescription():
    """A series description should describe a set of data without actually including the data.
        :param dataSource: str - The data's source (e.g. 'NOAATANDC')
        :param dataSeries: str - The series name (e.g. 'x_wind')
        :param dataLocation: str - The location of the data ('packChan')
        :param limit: timedelta - The max gap distance allowed for interpolation
        :param dataDatum: str = None
        :param dataIntegrityDescription: DataIntegrityDescription = None - An object that contains the data integrity calls that should be handled.
    """
    def __init__(self, dataSource: str, dataSeries: str, dataLocation: str, dataDatum: str = None,  dataIntegrityDescription: DataIntegrityDescription = None, verificationOverride: int = None) -> None:
        self.dataSource = dataSource
        self.dataSeries = dataSeries
        self.dataLocation = dataLocation
        self.dataDatum = dataDatum
        self.dataIntegrityDescription = dataIntegrityDescription
        self.verificationOverride = verificationOverride
       
        

    def __str__(self) -> str:
        return f'\n[SeriesDescription] -> source: {self.dataSource}, series: {self.dataSeries}, location: {self.dataLocation}, datum: {self.dataDatum}, dataIntegrityDescription: {self.dataIntegrityDescription}, verificationOverride: {self.verificationOverride}'
    
    def __repr__(self) -> str:
        return f'\nSeriesDescription({self.dataSource}, {self.dataSeries}, {self.dataLocation}, {self.dataDatum}, {self.dataIntegrityDescription}, {self.verificationOverride})'


class SemaphoreSeriesDescription():
    """A semaphore series description should describe a set of predictions that is actually generated by semaphore.
        :param modelName: str - The name of the model
        :param modelVersion: str - The version of the model
        :param dataSeries: str - The series name (e.g. 'x_wind')
        :param dataLocation: str - The location of the data ('packChan')
        :param dataLocation: str - The data's location
        :param dataDatum: str = None
    """
    def __init__(self, modelName: str, modelVersion: str, dataSeries: str, dataLocation: str, dataDatum: str = None) -> None:
        self.modelName = modelName
        self.modelVersion = modelVersion
        self.dataSeries = dataSeries
        self.dataLocation = dataLocation
        self.dataDatum = dataDatum

    def __str__(self) -> str:
        return f'\n[SemaphoreSeriesDescription] -> AIName: {self.modelName}, AIGeneratedVersion: {self.modelVersion}, DataSeries: {self.dataSeries}, Location: {self.dataLocation}, Datum: {self.dataDatum}'
    
    def __repr__(self) -> str:
        return f'\nSemaphoreSeriesDescription({self.modelName}, {self.modelVersion}, {self.dataSeries}, {self.dataLocation}, {self.dataDatum})'


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
    