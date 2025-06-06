# -*- coding: utf-8 -*-
#dspecParser.py
#----------------------------------
# Created By : Matthew Kastl
# Created Date: 4/28/2024
# version 2.0
#----------------------------------
""" The DPSEC or data specification file is a file containing all instruction and information
for semaphore to run a model. This file contains a parent parser that will execute sub parsers
for any version of despec. The dspec gets converted into objects found in this file.
 """ 
#----------------------------------
# 
#
#Imports


from os.path import exists
from json import load


class DSPEC_Parser:

    def parse_dspec(self, fPath: str) -> 'Dspec':

        if not exists(fPath):
            print(f'{fPath} not found!')
            raise FileNotFoundError
        with open(fPath) as dspecFile:
            
            # Read dspec from file and grab version
            dspec_json = load(dspecFile)
            self.__dspec_version = dspec_json.get('dspecVersion', '1.0')
            self.__dspec_json = dspec_json
        
        match self.__dspec_version:
            case '1.0':
                sub_parser = dspec_sub_Parser_1_0(self.__dspec_json)
            case '2.0':
                sub_parser = dspec_sub_Parser_2_0(self.__dspec_json)
            case _:
                raise NotImplementedError(f'No parser for dspec version {self.__dspec_version} not found!')
            
        return sub_parser.parse_dspec() 

        

class dspec_sub_Parser_1_0:

    def __init__(self, json: dict) -> None:
        self.__dspec_json = json
        self.__dspec = Dspec()

    def parse_dspec(self):
        self.__parse_meta()
        self.__parse_timing()
        self.__parse_output()
        self.__parse_inputs()
        return self.__dspec

    def __parse_meta(self):
        self.__dspec.modelName = self.__dspec_json["modelName"]
        self.__dspec.modelVersion = self.__dspec_json["modelVersion"]
        self.__dspec.author = self.__dspec_json["author"]
        self.__dspec.modelFileName = self.__dspec_json["modelFileName"]

    def __parse_timing(self):
        # Grab timing info from dict
        timingJson = self.__dspec_json["timingInfo"]
        
        # Parse
        timingInfo = TimingInfo()
        timingInfo.offset = timingJson["offset"]
        timingInfo.interval = timingJson["interval"]

        # Bind to dspec
        self.__dspec.timingInfo = timingInfo 
    
    def __parse_output(self):
        # Grab output info from dict
        outputJson = self.__dspec_json["outputInfo"]
        
        # Parse
        outputInfo = OutputInfo()
        outputInfo.outputMethod = outputJson["outputMethod"]
        outputInfo.leadTime = outputJson["leadTime"]
        outputInfo.series = outputJson["series"]
        outputInfo.location = outputJson["location"]
        outputInfo.interval = outputJson.get("interval")
        outputInfo.unit = outputJson.get("unit")
        outputInfo.datum = outputJson.get("datum")

        # Bind to dspec
        self.__dspec.outputInfo = outputInfo 


    def __parse_inputs(self) -> None:

            # Grab the list of inputs.
            # They are mostly the same as the new Dependent Series
            # Except for type, which has been moved to vector order
            # We also need to implicitly create outkeys for them
            # Such that they are compatible with the new ordered vector
            # The keys are just the order in which the inputs appear
            
            inputsJson = self.__dspec_json["inputs"]
            dependentSeriesList = []

            keys = []
            types = []
            indexes = []
            multipliedKeys = []
            ensembleMemberCount = []
            
            for idx, inputJson in enumerate(inputsJson):
                dseries = DependentSeries()
                dseries.name = inputJson["_name"]
                dseries.location = inputJson["location"]
                dseries.source = inputJson["source"]
                dseries.series = inputJson["series"]
                dseries.interval = inputJson["interval"]
                dseries.range = inputJson["range"]
                dseries.interpolationParameters = inputJson.get("interpolationParameters")
                dseries.datum = inputJson.get("datum")
                dseries.unit = inputJson.get("unit")
                dseries.verificationOverride = inputJson.get("verificationOverride")
                dseries.outKey = str(idx) # Assign it a key for ordered vector


                # We record what is needed for the ordered vector
                types.append(inputJson["type"])
                keys.append(str(idx))
                indexes.append((None, None))
                

                dependentSeriesList.append(dseries)
            # Bind to dspec
            self.__dspec.dependentSeries = dependentSeriesList 

            vOrder = VectorOrder()
            vOrder.keys = keys
            vOrder.dTypes = types
            vOrder.indexes = indexes
            vOrder.multipliedKeys = []
            vOrder.ensembleMemberCount = 1
            self.__dspec.orderedVector = vOrder



class dspec_sub_Parser_2_0:
    def __init__(self, json: dict) -> None:
        self.__dspec_json = json
        self.__dspec = Dspec()

    def parse_dspec(self):
        self.__parse_meta()
        self.__parse_timing()
        self.__parse_output()
        self.__parse_dependent_series()
        self.__parse_post_process_call()
        self.__parse_vector_order()
        return self.__dspec

    def __parse_meta(self):
        self.__dspec.modelName = self.__dspec_json["modelName"]
        self.__dspec.modelVersion = self.__dspec_json["modelVersion"]
        self.__dspec.author = self.__dspec_json["author"]
        self.__dspec.modelFileName = self.__dspec_json["modelFileName"]

    def __parse_timing(self):
        # Grab timing info from dict
        timingJson = self.__dspec_json["timingInfo"]
        
        # Parse
        timingInfo = TimingInfo()
        timingInfo.offset = timingJson["offset"]
        timingInfo.interval = timingJson["interval"]

        # Bind to dspec
        self.__dspec.timingInfo = timingInfo 
    
    def __parse_output(self):
        # Grab output info from dict
        outputJson = self.__dspec_json["outputInfo"]
        
        # Parse
        outputInfo = OutputInfo()
        outputInfo.outputMethod = outputJson["outputMethod"]
        outputInfo.leadTime = outputJson["leadTime"]
        outputInfo.series = outputJson["series"]
        outputInfo.location = outputJson["location"]
        outputInfo.interval = outputJson.get("interval")
        outputInfo.unit = outputJson.get("unit")
        outputInfo.datum = outputJson.get("datum")

        # Bind to dspec
        self.__dspec.outputInfo = outputInfo 

    def __parse_dependent_series(self):

            # Grab the list of dependant series
            dSeries_list = self.__dspec_json["dependentSeries"]
            dependentSeries = []
            
            # Parse Dependent Series
            for dSeries_dict in dSeries_list:
                dSeries = DependentSeries()
                dSeries.name = dSeries_dict.get("_name")
                dSeries.location = dSeries_dict["location"]
                dSeries.source = dSeries_dict["source"]
                dSeries.series = dSeries_dict["series"]
                dSeries.interval = dSeries_dict["interval"]
                dSeries.range = dSeries_dict["range"]
                dSeries.datum = dSeries_dict.get("datum")
                dSeries.unit = dSeries_dict.get("unit")
                dSeries.outKey = dSeries_dict.get("outKey")
                dSeries.verificationOverride = dSeries_dict.get("verificationOverride")

                # If there is a data Integrity Call we parse it, else its set to None.
                dataIntegrityDict = dSeries_dict.get("dataIntegrityCall")
                if dataIntegrityDict is None:
                    dSeries.dataIntegrityCall = None
                else:
                    dataIntegrityCall = DataIntegrityCall()
                    dataIntegrityCall.call = dataIntegrityDict['call']
                    dataIntegrityCall.args = dataIntegrityDict['args']
                    dSeries.dataIntegrityCall = dataIntegrityCall
                
                dependentSeries.append(dSeries)

            # Bind to dspec
            self.__dspec.dependentSeries = dependentSeries 

    def __parse_post_process_call(self):

            postProcessCalls = self.__dspec_json["postProcessCall"]
            parsedCalls = []
            for call in postProcessCalls:
                parsedCall = PostProcessCall()
                parsedCall.call = call['call']
                parsedCall.args = call.get('args', {})
                parsedCalls.append(parsedCall)
            
            self.__dspec.postProcessCall = parsedCalls

    def __parse_vector_order(self):

        vOrder: list['dict'] = self.__dspec_json["vectorOrder"]
        keys = []
        dTypes = []
        indexes = []
        multipliedKeys = []
        ensembleMemberCount = []             
        
        for dict in vOrder:
            keys.append(dict['key'])
            dTypes.append(dict['dType'])
            
            index: list[int] | None  = dict.get('indexes')

            # If there is no index object we will use None None to index the whole series
            if index is None: indexes.append((None, None))
            else: indexes.append(tuple(index))
            
            # Parsing for multiplied keys (AKA. Ensemble data)
            isMultipliedKey: bool = dict.get("isMultipliedKey", False) 
            if isMultipliedKey:
                multipliedKeys.append(dict['key']) 
                ensembleMember: int | None = dict.get("ensembleMemberCount", None) 
                if ensembleMemberCount is not None: ensembleMemberCount.append(ensembleMember)  

        # Throw parsing errors if the multiplied keys and ensemble member are not configured correctly
        if len(multipliedKeys) > 1:                         raise ValueError("DSPEC Parsing Error: More than one key has been marked multiplied. This is not supported by the current implementation!")
        if len(ensembleMemberCount) > 1:                    raise ValueError("DSPEC Parsing Error: More than one ensembleMemberCount has been detected. This is not supported by the current implementation!")
        if len(multipliedKeys) != len(ensembleMemberCount): raise ValueError("DSPEC Parsing Error: If there is a multiplied key there should be 1 and only 1 ensembleMemberCount!")
        
        vectorOrder = VectorOrder()
        vectorOrder.keys = keys
        vectorOrder.dTypes = dTypes
        vectorOrder.indexes = indexes
        vectorOrder.multipliedKeys = multipliedKeys
        vectorOrder.ensembleMemberCount = 1 if len(ensembleMemberCount) <= 0 else ensembleMemberCount[0]
        self.__dspec.orderedVector = vectorOrder
        

            
class Dspec:
    '''Parent Dspec class, includes header and metadata. Should include anything
    not in inputs or ouput info'''
    def __init__(self) -> None:
        self.modelName = None
        self.modelVersion = None
        self.author = None
        self.modelFileName = None
        
        self.outputInfo = None
        self.dependentSeries = []
        self.postProcessCall = []
        self.orderedVector = []
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

class DependentSeries:
    '''A dependant series is a described series to pull from Series provider.'''
    def __init__(self) -> None:
        self.name = None
        self.location = None
        self.source = None
        self.series = None
        self.unit = None
        self.datum = None
        self.interval = None
        self.range = None
        self.dataIntegrityCall = None
        self.outKey = None
        self.verificationOverride = None
        
        
    def __str__(self) -> str:
        return f'\n[DependentSeries] -> name: {self.name}, location: {self.location}, source: {self.source}, series: {self.series}, unit: {self.unit}, datum: {self.datum}, range: {self.range}, outkey: {self.outKey}, dataIntegrityCall: {self.dataIntegrityCall}, verificationOverride: {self.verificationOverride}'
    
    def __repr__(self):
        return f'\nDependentSeries({self.name}, {self.location}, {self.source}, {self.series}, {self.unit}, {self.datum}, {self.range}, {self.outKey},{self.dataIntegrityCall}, {self.verificationOverride})'
    
class PostProcessCall:
    '''All information pertaining to a required call to a post processing function'''
    def __init__(self) -> None:
        self.call = None
        self.args = None

    def __str__(self) -> str:
        return f'\n[PostProcessCall] -> call: {self.call}, args: {self.args}'
    
    def __repr__(self):
        return f'\nPostProcessCall({self.call}, {self.args})'
    
class DataIntegrityCall:
    def __init__(self) -> None:
            self.call = None
            self.args = None

    def __str__(self) -> str:
        return f'\n[DataIntegrityCall] -> call: {self.call}, args: {self.args}'
    
    def __repr__(self):
        return f'\nDataIntegrityCall({self.call}, {self.args})'
    
class VectorOrder:
    '''An object that holds the order and datatypes that the input vector should actually be'''
    def __init__(self) -> None:
        self.keys = []
        self.dTypes = []
        self.indexes = []
        self.multipliedKeys = []
        self.ensembleMemberCount = None

    def __str__(self) -> str:
        return f'\n[VectorOrder] -> keys: {self.keys}, dTypes: {self.dTypes}, indexes: {self.indexes}, multipliedKeys: {self.multipliedKeys}, ensembleMemberCount: {self.ensembleMemberCount}'
    
    def __repr__(self):
        return f'\nVectorOrder({self.keys}, {self.dTypes}, {self.indexes}, {self.multipliedKeys}, {self.ensembleMemberCount})'
    

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
    
    
    
