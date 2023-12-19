# -*- coding: utf-8 -*-
#apiDriver.py
#----------------------------------
# Created By: Beto Estrada Jr
# Created Date: 9/06/2023
#----------------------------------
"""This script acts as the driver for the FastAPI application.
 """ 
#----------------------------------
# 
#
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from datetime import datetime, timedelta

from DataClasses import SeriesDescription, SemaphoreSeriesDescription, TimeDescription
from SeriesProvider.SeriesProvider import SeriesProvider

load_dotenv()

app = FastAPI()

@app.get('/')
def read_main():
    return {'message': 'Hello World'}


@app.get('/input/source={source}/series={series}/location={location}/fromDateTime={fromDateTime}/toDateTime={toDateTime}')
async def get_input(source: str, series: str, location: str, fromDateTime: str, toDateTime: str, 
                    datum: str = None, interval: timedelta = None):
    """
    Retrieves input series object

    Args:

        - `source` (string): The data's source (e.g. "noaaT&C")

        - `series` (string): The series name (e.g. "dXWnCmp")

        - `location` (string): The location of the data (e.g. "packChan")

        - `fromDateTime` (string): "YYYYMMDDHH" Date to start at

        - `toDateTime` (string): "YYYYMMDDHH" Date to end at

        - `datum` (string): Optional

        - `interval` (int): Optional. The time step separating the data points in seconds (e.g. 3600 for hourly)

    Returns:
        Series: A series object holding either the requested data or an error message with the incomplete data. (src/DataClasses.py)

    Raises:
        HTTPException: If the series is not found.
    """
    try:
        fromDateTime = datetime.strptime(fromDateTime, '%Y%m%d%H')
        toDateTime = datetime.strptime(toDateTime, '%Y%m%d%H')
    except (ValueError, TypeError, OverflowError) as e:
        raise HTTPException(status_code=404, detail=f'{e}')
    
    requestDescription = SeriesDescription(
        source, 
        series,
        location, 
        datum
    )

    timeDescription = TimeDescription(
        fromDateTime, 
        toDateTime,
        interval
    )

    provider = SeriesProvider()
    responseSeries = provider.request_input(requestDescription, timeDescription)

    return responseSeries


@app.get('/output/ModelName={ModelName}/ModelVersion={ModelVersion}/series={series}/location={location}/ \
         /fromDateTime={fromDateTime}/toDateTime={toDateTime}')
async def get_output(ModelName: str, ModelVersion: str, series: str, location: str, fromDateTime: str, 
                     toDateTime: str, datum: str = None, interval: timedelta = None):
    """
    Retrieves output series object

    Args:

        - `ModelName` (string): The name of the model (e.g. "test AI")

        - `ModelVersion` (string): The version of the model (e.g. "1.0.0")

        - `series` (string): The series name (e.g. "waterHeight")

        - `location` (string): The location of the data (e.g. "packChan")

        - `fromDateTime` (string): "YYYYMMDDHH" Date to start at

        - `toDateTime` (string): "YYYYMMDDHH" Date to end at

        - `datum` (string): Optional

        - `interval` (int): Optional. The time step separating the data points in seconds (e.g. 3600 for hourly)

    Returns:
        Series: A series object holding either the requested data or an error message with the incomplete data. (src/DataManagement/DataClasses.py)

    Raises:
        HTTPException: If the series is not found.
    """ 
    try:
        fromDateTime = datetime.strptime(fromDateTime, '%Y%m%d%H')
        toDateTime = datetime.strptime(toDateTime, '%Y%m%d%H')
    except (ValueError, TypeError, OverflowError) as e:
        raise HTTPException(status_code=404, detail=f'{e}')
    
    requestDescription = SemaphoreSeriesDescription(
        ModelName, 
        ModelVersion, 
        series, 
        location,
        datum
    )

    timeDescription = TimeDescription(
        fromDateTime,
        toDateTime,
        interval
    )

    provider = SeriesProvider()
    responseSeries = provider.request_output(requestDescription, timeDescription)

    return responseSeries