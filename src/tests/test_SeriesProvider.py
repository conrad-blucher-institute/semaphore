# -*- coding: utf-8 -*-
# test_SeriesProvider.py
#-------------------------------
# Created By : Matthew Kastl
# Created Date: 10/24/2023
# version 1.0
#-------------------------------
""" This file tests the series provider class
 """ 
#-------------------------------
# 
#
#Imports
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from dotenv import load_dotenv
    
from SeriesProvider.SeriesProvider import SeriesProvider

### Having import problems
# def test__merge_results():

#     load_dotenv()
#     seriesProvider = SeriesProvider()

#     list1 = [1, 2, 3, 4, 5]
#     list2 = [5, 6]

#     result = seriesProvider._SeriesProvider__merge_results(list1, list2)
#     assert result == [1, 2, 3, 4, 5, 6]

# test__merge_results()