# -*- coding: utf-8 -*-
#LIGHTHOUSE.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 11/3/2023
# version 1.0
#----------------------------------
""" This file ingests data from CBI maintained Lighthouse
 """ 
#----------------------------------
# 
#
#Input
from SeriesStorage.ISeriesStorage import series_storage_factory
from DataClasses import Series, SeriesDescription, Input, TimeDescription
from DataIngestion.IDataIngestion import IDataIngestion
from utility import log

from datetime import datetime
from urllib.error import HTTPError
from urllib.request import urlopen
import json


class LIGHTHOUSE(IDataIngestion):

    
    def __init__(self):
        pass
    
    def validate_series(self, series: Series) -> bool:
        raise NotImplementedError
