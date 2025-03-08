# -*- coding: utf-8 -*-
# WEATHERCOMPANY.py
#-------------------------------
# Created By : Savannah Stephenson
# Version : 1.0
#-------------------------------
""" This file tests the series provider class
""" 
#-------------------------------
# 
#
#Imports
from SeriesStorage.ISeriesStorage import series_storage_factory
from DataClasses import Series, SeriesDescription, Input, TimeDescription
from DataIngestion.IDataIngestion import IDataIngestion
from utility import log

from datetime import datetime
from urllib.error import HTTPError
from urllib.request import urlopen
import json