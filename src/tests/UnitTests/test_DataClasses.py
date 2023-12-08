# -*- coding: utf-8 -*-
#test_DataClasses.py
#-------------------------------
# Created By: Beto Estrada
# Created Date: 10/24/2023
# version 1.0
#----------------------------------
"""This file tests the DataClasses module
 """ 
#----------------------------------
# 
#
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from datetime import datetime, timedelta

from src.DataClasses import Input

def test_createInput():
    currentDate = datetime.now()
    pastDate = currentDate - timedelta(days=7)

    input = Input(dataValue = '1.028', 
                  dataUnit = 'meter', 
                  timeVerified = pastDate,
                  timeGenerated = pastDate, 
                  longitude = '-97.318',
                  latitude = '27.4844')
    
    assert input.dataValue == '1.028'
    assert input.dataUnit == 'meter'
    assert input.timeVerified == pastDate
    assert input.timeGenerated == pastDate
    assert input.longitude == '-97.318'
    assert input.latitude == '27.4844'

def test_InputEquivalence():
    currentDate = datetime.now()
    pastDate = currentDate - timedelta(days=7)

    input1 = Input(dataValue = '1.028', 
                  dataUnit = 'meter', 
                  timeVerified = pastDate,
                  timeGenerated = pastDate, 
                  longitude = '-97.318',
                  latitude = '27.4844')
    
    input2 = Input(dataValue = '1.028', 
                  dataUnit = 'meter', 
                  timeVerified = pastDate,
                  timeGenerated = pastDate, 
                  longitude = '-97.318',
                  latitude = '27.4844')
    
    assert input1 == input2

