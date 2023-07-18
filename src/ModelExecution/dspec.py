# -*- coding: utf-8 -*-
#inputGatherer.py
#----------------------------------
# Created By : Matthew Kastl
# Created Date: 2/3/2023
# version 1.0
#----------------------------------
""" This file should be a python class that matches a despec
one to one for use in modelExecution.
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
        self.datum = None

    def __str__(self) -> str:
        return f'[OutputInfo] -> outputMethod: {self.outputMethod}, leadTime: {self.leadTime}, series: {self.series}, location: {self.location}, datum: {self.datum}'
    
    def __repr__(self):
        return f'[OutputInfo] -> outputMethod: {self.outputMethod}, leadTime: {self.leadTime}, series: {self.series}, location: {self.location}, datum: {self.datum}'

class Input:
    '''input object should contain everything that could be contained
    in a dspec input object'''
    def __init__(self) -> None:
        self.name = None
        self.location = None
        self.source = None
        self.series = None
        self.unit = None
        self.type = None
        self.datum = None
        self.range = None

    def __str__(self) -> str:
        return f'[Input] -> name: {self.name}, location: {self.location}, source: {self.source}, series: {self.series}, unit: {self.unit}, type: {self.type}, datum: {self.datum}, range: {self.range}'
    
    def __repr__(self):
        return f'[Input] -> name: {self.name}, location: {self.location}, source: {self.source}, series: {self.series}, unit: {self.unit}, type: {self.type}, datum: {self.datum}, range: {self.range}'