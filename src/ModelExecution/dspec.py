# -*- coding: utf-8 -*-
#inputGatherer.py
#----------------------------------
# Created By : Matthew Kastl
# Created Date: 2/3/2023
# version 1.0
#----------------------------------
""" This file should a one for one match what a dspec looks like such that it can be passes around and worked with easily.
 """ 
#----------------------------------
# 
#
#Imports
class Dspec:
    '''Parent Dspec class, includes header and metadata. Should include anything
    not in inputs or ouput info'''
    def __init__(self) -> None:
        self.modelName = None
        self.modelVersion = None
        self.author = None
        self.modelFileName = None
        
        self.outputInfo = None
        self.inputs = []

    def __str__(self) -> str:
        return f'[Dspec] -> modelName: {self.modelName}, modelVersion: {self.modelVersion}, author: {self.author}, modelFileName: {self.modelFileName}'

class OutputInfo:
    '''OuputInfo object should contain everything that could be contained
    in a dspec outputInfo object'''
    def __init__(self) -> None:
        self.outputMethod = None
        self.leadTime = None
        self.series = None
        self.location = None
        self.interval = None
        self.fromDateTime = None
        self.toDateTime = None
        self.datum = None
        self.unit = None

    def __str__(self) -> str:
        return f'[OutputInfo] -> outputMethod: {self.outputMethod}, leadTime: {self.leadTime}, series: {self.series}, location: {self.location}, interval: {self.interval}, fromDateTime: {self.fromDateTime}, toDateTime: {self.toDateTime}, datum: {self.datum}, unit: {self.unit}'
    
    def __repr__(self):
        return f'[OutputInfo] -> outputMethod: {self.outputMethod}, leadTime: {self.leadTime}, series: {self.series}, location: {self.location}, interval: {self.interval}, fromDateTime: {self.fromDateTime}, toDateTime: {self.toDateTime}, datum: {self.datum}, unit: {self.unit}'

class Input:
    '''input object should contain everything that could be contained
    in a dspec input object'''
    def __init__(self) -> None:
        self.name = None
        self.location = None
        self.source = None
        self.series = None
        self.dataClassification = None
        self.unit = None
        self.type = None
        self.datum = None
        self.interval = None
        self.range = None

    def __str__(self) -> str:
        return f'[Input] -> name: {self.name}, location: {self.location}, source: {self.source}, series: {self.series}, dataClassification: {self.dataClassification}, unit: {self.unit}, type: {self.type}, datum: {self.datum}, range: {self.range}'
    
    def __repr__(self):
        return f'[Input] -> name: {self.name}, location: {self.location}, source: {self.source}, series: {self.series}, dataClassification: {self.dataClassification}, unit: {self.unit}, type: {self.type}, datum: {self.datum}, range: {self.range}'