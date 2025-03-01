# -*- coding: utf-8 -*-
#test_InputGatherer.py
#-------------------------------
# Created By: Matthew Kastl
# Created Date: 5/14/2024
# version 1.0
#----------------------------------
"""This file tests the dspec parsing parsing a 1.0 and 2.0 dspec
 """ 
#----------------------------------
# 
#
import sys
sys.path.append('/app/src')

from json import load
from os import path, getenv
import pytest

from src.utility import log, construct_true_path
from src.ModelExecution.inputGatherer import InputGatherer

@pytest.mark.parametrize("dspecFilePath", [
     ('./data/dspec/TestModels/test_dspec.json'),
     ('./data/dspec/TestModels/test_dspec-2-0.json'),
     ('./data/dspec/TestModels/test_multiVector.json')
])
def test_parseDSPEC(dspecFilePath: str):
    """This function tests whether the specified DSPEC file
    is correctly parsed in the InputGatherer object
    """

    inputGatherer = InputGatherer(dspecFilePath)

    # Read dspec from file and grab version
    with open(dspecFilePath) as dspecFile:
        dspec_json = load(dspecFile)
    dspec_version = dspec_json.get('dspecVersion', '1.0')
    match dspec_version:
        case '1.0':
            sub_test_dspec_1_0(inputGatherer, dspecFilePath)
        case '2.0':
            sub_test_dspec_2_0(inputGatherer, dspecFilePath)
        case _:
            raise NotImplementedError(f'No parser for dspec version {dspec_version} not found!')
    
    assert True


def sub_test_dspec_1_0(inputGatherer: InputGatherer, dspecFilePath: str):
    dspec = inputGatherer.get_dspec()
    with open(dspecFilePath) as dspecFile:
        json = load(dspecFile)
        # Metadata
        assert dspec.modelName == json['modelName']
        assert dspec.modelVersion == json['modelVersion']
        assert dspec.author == json['author']
        assert dspec.modelFileName == json['modelFileName']

        # OutputInfo
        outputJson = json["outputInfo"]
        outputInfo = dspec.outputInfo
        assert outputInfo.outputMethod == outputJson["outputMethod"]
        assert outputInfo.leadTime == outputJson["leadTime"]
        assert outputInfo.series == outputJson["series"]
        assert outputInfo.location == outputJson["location"]
        assert outputInfo.interval == outputJson["interval"]
        assert outputInfo.unit == outputJson["unit"]

        # DSPEC 1.0 has Inputs (Only first one)
        inputsJson = json["inputs"]
        dependentSeries = dspec.dependentSeries[0]
        assert dependentSeries.location == inputsJson[0]["location"]
        assert dependentSeries.source == inputsJson[0]["source"]
        assert dependentSeries.series == inputsJson[0]["series"]
        assert dependentSeries.interval == inputsJson[0]["interval"]
        assert dependentSeries.range == inputsJson[0]["range"]

        # Dependent Series Count
        assert len(dspec.dependentSeries) == 9

        # There should be no post process calls in 1.0
        assert len(dspec.postProcessCall) == 0

        # Vector order (Only the first one)
        inputsJson = json["inputs"]
        vectorOrder = dspec.orderedVector
        assert vectorOrder.keys[0] == '0'
        assert vectorOrder.dTypes[0] == inputsJson[0]["type"]

        # Vector order Count
        assert len(dspec.orderedVector.keys) == 9
        assert len(dspec.orderedVector.dTypes) == 9



def sub_test_dspec_2_0(inputGatherer: InputGatherer, dspecFilePath: str):
    dspec = inputGatherer.get_dspec()
    with open(dspecFilePath) as dspecFile:
        json = load(dspecFile)
        # Metadata
        assert dspec.modelName == json['modelName']
        assert dspec.modelVersion == json['modelVersion']
        assert dspec.author == json['author']
        assert dspec.modelFileName == json['modelFileName']

        # OutputInfo
        outputJson = json["outputInfo"]
        outputInfo = dspec.outputInfo
        assert outputInfo.outputMethod == outputJson["outputMethod"]
        assert outputInfo.leadTime == outputJson["leadTime"]
        assert outputInfo.series == outputJson["series"]
        assert outputInfo.location == outputJson["location"]
        assert outputInfo.interval == outputJson["interval"]
        assert outputInfo.unit == outputJson["unit"]

        # dependentSeries (Only first one)
        dependentSeriesJson = json["dependentSeries"]
        dependentSeries = dspec.dependentSeries[0]
        assert dependentSeries.location == dependentSeriesJson[0]["location"]
        assert dependentSeries.source == dependentSeriesJson[0]["source"]
        assert dependentSeries.series == dependentSeriesJson[0]["series"]
        assert dependentSeries.interval == dependentSeriesJson[0]["interval"]
        assert dependentSeries.range == dependentSeriesJson[0]["range"]

        # Dependent Series Count
        assert len(dspec.dependentSeries) == 5

        #Post process call (Only first one)
        postProcessCallJSON = json["postProcessCall"]
        postProcessCall = dspec.postProcessCall[0]
        assert postProcessCall.call == postProcessCallJSON[0]["call"]
        assert postProcessCall.args == postProcessCallJSON[0]["args"]
        # Post process Call count
        assert len(dspec.postProcessCall) == 2

        # Vector order (Only the first one)
        vectorOrderJson = json["vectorOrder"]
        vectorOrder = dspec.orderedVector
        assert vectorOrder.keys[0] == vectorOrderJson[0]["key"]
        assert vectorOrder.dTypes[0] == vectorOrderJson[0]["dType"]

        vectorSpecificationsJson = json.get("vectorSpecifications", {})
        assert vectorOrder.multipliedKeys == vectorSpecificationsJson.get("multipliedKeys", [])
        assert vectorOrder.amntExpectedVectors == vectorSpecificationsJson.get("amntExpectedVectors", None)
        
        # Vector order Count
        assert len(dspec.orderedVector.keys) == 9
        assert len(dspec.orderedVector.dTypes) == 9
