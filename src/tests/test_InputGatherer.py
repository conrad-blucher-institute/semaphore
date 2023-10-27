# -*- coding: utf-8 -*-
#test_InputGatherer.py
#-------------------------------
# Created By: Beto Estrada
# Created Date: 10/24/2023
# version 1.0
#----------------------------------
"""This file tests the InputGatherer module
 """ 
#----------------------------------
# 
#
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from datetime import datetime
from json import load
from os import path, getenv

from dotenv import load_dotenv

load_dotenv()

from utility import log, construct_true_path
from ModelExecution.inputGatherer import InputGatherer

def test_parseDSPEC():
    """This function tests whether the specified DSPEC file
    is correctly parsed in the InputGatherer object
    """
    dspecFileName = 'test_dspec.json'

    inputGatherer = InputGatherer(dspecFileName)

    dspecFilePath = construct_true_path(getenv('DSPEC_FOLDER_PATH')) + dspecFileName
    if not path.exists(dspecFilePath):
        log(f'{dspecFilePath} not found!')
        raise FileNotFoundError
    
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

        # InputInfo (Only first one)
        inputsJson = json["inputs"]
        inputInfo = dspec.inputs[0]
        assert inputInfo.name == inputsJson[0]["_name"]
        assert inputInfo.location == inputsJson[0]["location"]
        assert inputInfo.source == inputsJson[0]["source"]
        assert inputInfo.series == inputsJson[0]["series"]
        assert inputInfo.type == inputsJson[0]["type"]
        assert inputInfo.interval == inputsJson[0]["interval"]
        assert inputInfo.range == inputsJson[0]["range"]

    
def test_generateInputSpecifications():
    """This function tests whether __generate_inputSpecifications
    generates the correct input specifications from the specified
    DSPEC file
    """
    currentDate = datetime.now()

    dspecFileName = 'test_dspec.json'

    inputGatherer = InputGatherer(dspecFileName)

    inputGatherer.__generate_inputSpecifications(currentDate)

    inputSpecifications = inputGatherer.get_input_specifications()

    specification = inputSpecifications[0]
    seriesDescription = specification[0]
    timeDescription = specification[1]
    dataType = specification[2]

    dspecFilePath = construct_true_path(getenv('DSPEC_FOLDER_PATH')) + dspecFileName
    if not path.exists(dspecFilePath):
        log(f'{dspecFilePath} not found!')
        raise FileNotFoundError
    
    with open(dspecFilePath) as dspecFile:
        json = load(dspecFile)

        inputsJson = json["inputs"]

        assert seriesDescription.dataSource == inputsJson[0]["source"]
        assert seriesDescription.dataSeries == inputsJson[0]["series"]
        assert seriesDescription.dataLocation == inputsJson[0]["location"]
        assert seriesDescription.dataDatum == inputsJson[0].get("datum")
        assert timeDescription.interval == inputsJson[0]["interval"]
        assert dataType == inputsJson[0]["type"]