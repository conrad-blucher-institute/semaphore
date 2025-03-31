# -*- coding: utf-8 -*-
#test_NDFD.py
#-------------------------------
# Created By: Beto Estrada
# Created Date: 11/03/2023
# version 1.0
#----------------------------------
"""This file tests the NDFD ingestion class and its functions. 
 """ 
#----------------------------------
# 
#
#Imports
import sys
import sys
sys.path.append('/app/src')

from datetime import datetime, timedelta
from dotenv import load_dotenv
import pytest

from DataClasses import TimeDescription, SeriesDescription
from DataIngestion.DI_Classes.NDFD import NDFD

load_dotenv()
@pytest.mark.parametrize("data_dictionary, toDateTime", [
    ([[1704520800, 13], [1704531600, 12]], datetime(2024, 1, 6, 2)),
    ([[1704531600, 12]], datetime(2024, 1, 6, 2)),
    ([[1704520800, 13]], datetime(2024, 1, 6, 2)),
])
def test_find_closest_average(data_dictionary: list, toDateTime: datetime):
    """This function tests whether the find_closest_average function in NDFD correctly
    calculates the data point for a datetime that does not exist in a nested list using
    the average of the data points surrounding the given datetime. If only one other datetime
    exists in the list, then the value for that datetime is used as the average.
    """
    ingestSeries = NDFD()

    closest_average = ingestSeries.find_closest_average(data_dictionary, int(toDateTime.timestamp()))

    if closest_average is None: 
        raise ValueError('Error calculating average. Test should not return None based on given parameters. However, None was returned!')
    
    # Add toDateTime and averaged data point to data_dictionary
    data_dictionary.append([int(toDateTime.timestamp()), closest_average])

    # Checks if toDateTime exists in data_dictionary
    toDateTime_exists = any(timestamp[0] == int(toDateTime.timestamp()) for timestamp in data_dictionary)

    assert toDateTime_exists is True