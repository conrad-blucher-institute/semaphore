# -*- coding: utf-8 -*-
#DataClasses.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/30/2023
# version 2.0
#----------------------------------
"""This file holds for classes that the system will use to discribe series of data, actuals, and predictions.
 """ 
#----------------------------------
# 
#
#Inports
from datetime import datetime
from typing import List


class Actual():
    '''An Actual is a real data point with a value and a unit and can be linked to a date time.'''
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
    '''An Pridiction is a Is a predicted value. It has all the same things as an Actual, but it decribes its time as the time the prediction
    was made and the lead time till its verification time.
    generatedTime + leadTime = verificationTime
    '''
    def __init__(self, value: str, unit: str, leadTime: float, generatedTime: datetime, successValue: str = None, longitude: str = None, latitude: str = None) -> None:
        self.value = value
        self.unit = unit
        self.leadTime = leadTime
        self.generatedTime = generatedTime
        self.successValue = successValue
        self.longitude = longitude
        self.latitude = latitude
    
    def __str__(self) -> str:
        return f'\n[Prediction] -> value: {self.value}, unit: {self.unit}, leadTime: {self.leadTime}, generatedTime: {self.generatedTime}, successValue {self.successValue}'
    
    def __repr__(self):
        return f'\n[Prediction] -> value: {self.value}, unit: {self.unit}, leadTime: {self.leadTime}, generatedTime: {self.generatedTime}, successValue {self.successValue}'
    
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
    '''A series description should describe a set of data wihtout actually including the data.'''
    def __init__(self, source: str, series: str, location: str, unit: str, interval: int, fromDateTime: datetime, toDateTime: datetime, datum: str = None) -> None:
        self.source = source
        self.series = series
        self.location = location
        self.unit = unit
        self.datum = datum
        self.interval = interval
        self.fromDateTime = fromDateTime
        self.toDateTime = toDateTime

    def __str__(self) -> str:
        return f'\n[SeriesDescription] -> source: {self.source}, series: {self.series}, location: {self.location}, unit: {self.unit}, datum {self.datum}, interval: {self.interval}, fromDateTime {self.fromDateTime}, toDateTime {self.toDateTime}'
    
    def __repr__(self) -> str:
        return f'\n[SeriesDescription] -> source: {self.source}, series: {self.series}, location: {self.location}, unit: {self.unit}, datum {self.datum}, interval: {self.interval}, fromDateTime {self.fromDateTime}, toDateTime {self.toDateTime}'

class LocalSeriesDescription():
    '''A local series description should describe a set of predictions that is actually generated by semaphore.'''
    def __init__(self, ModelName: str, ModelVersion: str, series:str, location: str, datum: str = None) -> None:
        self.ModelName = ModelName
        self.ModelVersion = ModelVersion
        self.series = series
        self.location = location
        self.datum = datum

    def __str__(self) -> str:
        return f'\n[LocalSeriesDescription] -> AIName: {self.ModelName}, AIGeneratedVersion: {self.ModelVersion}, location: {self.location}, datum: {self.datum}'
    
    def __repr__(self) -> str:
        return f'\n[LocalSeriesDescription] -> AIName: {self.ModelName}, AIGeneratedVersion: {self.ModelVersion}, location: {self.location}, datum: {self.datum}'

class Series():
    '''A series is a pairing of an object discribing a series of data, and an array of the data itself. It also includes some metta data about how well the data provided matches the discription.'''
    def __init__(self, description: SeriesDescription | LocalSeriesDescription, isComplete: bool, nonCompleteReason: Exception = None) -> None:
        self.description = description
        self.isComplete = isComplete
        self.nonCompleteReason = nonCompleteReason
        self.__data = []

    def bind_data(self, data: List[Actual | Prediction]) -> None:
        self.__data = data

    def get_data(self) -> List[Actual | Prediction] | None:
        return self.__data

    def __str__(self) -> str:
        return f'\n[Series] -> description: {self.description}, wasSuccessful: {self.isComplete}, errorReason: {self.nonCompleteReason}'
    
