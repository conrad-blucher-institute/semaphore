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

from DataClasses import Input

def test_createInput():
    currentDate = datetime.now()
    pastDate = currentDate - timedelta(days=7)

    input = Input(value = '1.028', 
                  unit = 'meter', 
                  timeGenerated = pastDate, 
                  timeAcquired = currentDate, 
                  timeVerified = pastDate,
                  longitude = '-97.318',
                  latitude = '27.4844')

    assert True