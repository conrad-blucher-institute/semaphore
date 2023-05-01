# -*- coding: utf-8 -*-
#DataIngestionMap.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/30/2023
# version 1.0
#----------------------------------
"""This file holds for classes that each act as a structer for the semaphore program. A request is a single request for something like a single input specification. 
A response is the obv response to the request. A Data point is a static point of data with a single datetime of when it was actualized.
A Prediction is a datapoint that was predicted by an AI. Includes the datetime of when it was generated and the lead time of the span from when the prediction was made
to when it could be verified.
 """ 
#----------------------------------
# 
#
#Inports
from datetime import datetime

class DataPoint():
    def __init__(self, value: str, unit: str, dateTime: datetime) -> None:
        self.value = value
        self.unit = unit
        self.dateTime = dateTime

    def __str__(self) -> str:
        return f'[DataPoint] -> value: {self.value}, unit: {self.unit}, dataTime: {self.dateTime}'
    
    def __repr__(self):
        return f'[DataPoint] -> value: {self.value}, unit: {self.unit}, dataTime: {self.dateTime}'

class Prediction():
    def __init__(self, value: str, unit: str, leadTime: float, generatedTime: datetime, successValue: str = None) -> None:
        self.value = value
        self.unit = unit
        self.leadTime = leadTime
        self.generatedTime = generatedTime
        self.successValue = successValue
    
    def __str__(self) -> str:
        return f'[Prediction] -> value: {self.value}, unit: {self.unit}, leadTime: {self.leadTime}, generatedTime: {self.generatedTime}, successValue {self.successValue}'
    
    def __repr__(self):
        return f'[Prediction] -> value: {self.value}, unit: {self.unit}, leadTime: {self.leadTime}, generatedTime: {self.generatedTime}, successValue {self.successValue}'


class Request():
    def __init__(self, source: str, series: str, location: str, unit: str, fromDateTime: datetime, toDateTime: datetime, datum: str = None) -> None:
        self.source = source
        self.series = series
        self.location = location
        self.unit = unit
        self.datum = datum
        self.fromDateTime = fromDateTime
        self.toDateTime = toDateTime

    def __str__(self) -> str:
        return f'[Request] -> source: {self.source}, series: {self.series}, location: {self.location}, unit: {self.unit}, datum {self.datum}, fromDateTime {self.fromDateTime}, toDateTime {self.toDateTime}'


class Response():
    def __init__(self, request: Request, wasSuccessful: bool, errorReason: str = None) -> None:
        self.request = request
        self.data = []
        self.wasSuccessful = wasSuccessful
        self.errorReason = errorReason

    def __str__(self) -> str:
        return f'[Response] -> request: {self.request}, data: {[d for d in self.data]}, wasSuccessful: {self.wasSuccessful}, errorReason: {self.errorReason}'
    
