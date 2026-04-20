# -*- coding: utf-8 -*-
#test_dspecParser.py
#-------------------------------
# Created By: Matthew Kastl
# Created Date: 5/14/2024
# version 1.0
#----------------------------------
"""This file tests the dspec parsing parsing a 1.0 and 2.0 dspec

docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_dspecParser.py
 """ 
#----------------------------------
# 
#
import sys
sys.path.append('/app/src')

from json import load
import pytest
from src.ModelExecution.dspecParser import DSPEC_Parser

@pytest.mark.parametrize("dspecFilePath", [
     ('./data/dspec/TestModels/test_dspec-2-0.json'),
     ('./data/dspec/TestModels/test_dspec-2-1.json'),
     ('./data/dspec/TestModels/test_multiVector.json'),
])
def test_parseDSPEC(dspecFilePath: str):
    """This function tests whether the specified DSPEC file
    is correctly parsed in the InputGatherer object
    """

    # Read dspec from file and grab version
    with open(dspecFilePath) as dspecFile:
        dspec_json = load(dspecFile)
    dspec_version = dspec_json.get('dspecVersion', '2.0')
    if dspec_version.startswith('2.'):
        sub_test_dspec_2_0(dspecFilePath)
    else:
        raise NotImplementedError(f'No parser for dspec version {dspec_version} not found!')

    assert True

def test_expected_output_shape_defaults_from_file():
    dspecFilePath = "./data/dspec/TestModels/test_missingExpectedOutputShape.json"

    # Sanity check: confirm the key is actually missing in the file
    with open(dspecFilePath) as f:
        dspec_json = load(f)

    output_json = dspec_json.get("outputInfo", {})
    assert "expectedOutputShape" not in output_json or output_json["expectedOutputShape"] is None

    # Parse and verify defaults
    dspec = DSPEC_Parser().parse_dspec(dspecFilePath)
    eos = dspec.outputInfo.expectedOutputShape
    assert eos.memberCount == 1
    assert eos.inputVectorCount == 1
    assert eos.outputsPerVector == 1

def test_invalid_vector_order():
    """Test that a DSPEC file with invalid vector order fails."""
    dspecFilePath = './data/dspec/TestModels/test_invalidVectorOrder.json'

    with open(dspecFilePath) as f:
        dspec_json = load(f)
    assert dspec_json is not None  
    with pytest.raises(
        ValueError,
        match=r"DSPEC Parsing Error: More than one key has been marked multiplied\. This is not supported by the current implementation!"
    ):
        dspecParser = DSPEC_Parser()
        dspecParser.parse_dspec(dspecFilePath)


def sub_test_dspec_2_0(dspecFilePath: str):

    dspecParser = DSPEC_Parser()
    dspec = dspecParser.parse_dspec(dspecFilePath)
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
        statistics = outputJson.get("statistics", None)
        assert outputInfo.statistics == statistics
        expected = outputJson.get("expectedOutputShape", None)
        eos = outputInfo.expectedOutputShape
        if expected == None:
            assert eos.memberCount == 1
            assert eos.inputVectorCount == 1
            assert eos.outputsPerVector == 1
        else:
            assert eos.memberCount == expected["memberCount"]
            assert eos.inputVectorCount == expected["inputVectorCount"]
            assert eos.outputsPerVector == expected["outputsPerVector"]
        

        # Dependent Series Count
        assert len(dspec.dependentSeries) == 5

        #Post process call (Only first one)
        postProcessCallJSON = json["postProcessCall"]
        postProcessCall = dspec.postProcessCall[0]
        assert postProcessCall.call == postProcessCallJSON[0]["call"]
        assert postProcessCall.args == postProcessCallJSON[0]["args"]
        # Post process Call count
        assert len(dspec.postProcessCall) == 2

        # Vector order 
        vectorOrderJson = json["vectorOrder"]
        vectorOrder = dspec.orderedVector
        assert vectorOrder.keys[0] == vectorOrderJson[0]["key"]
        assert vectorOrder.dTypes[0] == vectorOrderJson[0]["dType"]
        
        # Iterate through vectorOrderJson to ensure all values match
        for i, entry in enumerate(vectorOrderJson): 
            key = entry.get("key", [])  
            if entry.get("isMultipliedKey", False):
                assert vectorOrder.multipliedKeys[i] == key # Default to empty list
   
        
        # Vector order Count
        assert len(dspec.orderedVector.keys) == 9
        assert len(dspec.orderedVector.dTypes) == 9

    