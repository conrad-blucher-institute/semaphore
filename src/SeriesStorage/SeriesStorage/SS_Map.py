# -*- coding: utf-8 -*-
#SS_Map.py
#-------------------------------
# Created By : Matthew Kastl
# Created Date: 8/24/2023
# version 1.0
#-------------------------------
""" This scrips maps the code to an instance of the ISeriesStorage class
 """ 
#-------------------------------
# 
#
#Imports
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from SeriesStorage.ISeriesStorage import ISeriesStorage
from SeriesStorage.SeriesStorage.SS_SQLLite import SS_SQLLite
def map_to_SS_Instance() -> ISeriesStorage:
    return SS_SQLLite()
