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
        self.timingInfo = None

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
        return f'\n[OutputInfo] -> outputMethod: {self.outputMethod}, leadTime: {self.leadTime}, series: {self.series}, location: {self.location}, interval: {self.interval}, fromDateTime: {self.fromDateTime}, toDateTime: {self.toDateTime}, datum: {self.datum}, unit: {self.unit}'
    
    def __repr__(self):
        return f'\nOutputInfo({self.outputMethod}, {self.leadTime}, {self.series}, {self.location}, {self.interval}, {self.fromDateTime}, {self.toDateTime}, {self.datum}, {self.unit})'

class InputInfo:
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
        self.interval = None
        self.range = None
        self.verificationOverride = None

    def __str__(self) -> str:
        return f'\n[InputInfo] -> name: {self.name}, location: {self.location}, source: {self.source}, series: {self.series}, unit: {self.unit}, type: {self.type}, datum: {self.datum}, range: {self.range}, verificationOverride: {self.verificationOverride}'
    
    def __repr__(self):
        return f'\nInputInfo({self.name}, {self.location}, {self.source}, {self.series}, {self.unit}, {self.type}, {self.datum}, {self.range}, {self.verificationOverride})'
    
class TimingInfo:
    """Timing info should contain everything that could be obtained in a dspec timing info object
    """
    def __init__(self) -> None:
        self.offset = None
        self.interval = None
        
    def __str__(self) -> str:
        return f'\n[TimingInfo] -> offset: {self.offset}, interval: {self.interval})'
    
    def __repr__(self):
        return f'\nTimingInfo({self.offset},{self.interval})'
    
    