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
from fastapi import FastAPI, HTTPException, Query

from datetime import datetime, timedelta
from DataClasses import SeriesDescription, SemaphoreSeriesDescription, TimeDescription
from SeriesProvider.SeriesProvider import SeriesProvider

load_dotenv()

app = FastAPI(root_path='/semaphore-api',)

@app.get('/')
def read_main():
    return {'message': 'Hello World'}


@app.get('/input/source={source}/series={series}/location={location}/fromDateTime={fromDateTime}/toDateTime={toDateTime}')
async def get_input(source: str, series: str, location: str, fromDateTime: str, toDateTime: str, 
                    datum: str = None, interval: int = None):
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
    
    if interval is not None:
        interval = timedelta(seconds=interval)
    
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
    responseSeries = provider.request_input(requestDescription, timeDescription, saveIngestion=False)

    # Protect the API's JSON ENCODER from freaking out about floats sneaking from the ingestion class
    responseSeries.dataFrame['dataValue'] = responseSeries.dataFrame['dataValue'].astype(str)

    return responseSeries


@app.get('/output_latest/')
async def get_outputs_latest(modelNames: list[str] = Query(None)):
    """
    Queries outputs for a given models looking for the last prediction that was made with them.
    Args:
        - `modelNames` (string): The name of the model (e.g. "test AI"), you can repeat this parameter to request multiple models

    Returns:
        The results for each model index by the model name. If no data can be found for that model the value will be null.
    """ 

    provider = SeriesProvider()
    results = {}
    for modelName in modelNames:
        results[modelName] = provider.request_output('LATEST', model_name= modelName)
    return results


@app.get('/output_time_span/')
async def get_outputs_time_span(fromDateTime: str, toDateTime: str, modelNames: list[str] = Query(None)):
    """
    Queries outputs for a given time range for given models.
    Args:
        - `fromDateTime` (string): "YYYYMMDDHH" Date to start at
        - `toDateTime` (string): "YYYYMMDDHH" Date to end at
        - `modelNames` (string): The name of the model (e.g. "test AI"), you can repeat this parameter to request multiple models

    Returns:
        The results for each model index by the model name. If no data can be found for that model for the provided time range the value will be null.
    """ 
    try:
        fromDateTime = datetime.strptime(fromDateTime, '%Y%m%d%H')
        toDateTime = datetime.strptime(toDateTime, '%Y%m%d%H')
    except (ValueError, TypeError, OverflowError) as e:
        raise HTTPException(status_code=404, detail=f'{e}')

    provider = SeriesProvider()
    results = {}
    for modelName in modelNames:
        results[modelName] = provider.request_output('TIME_SPAN', model_name=modelName, from_time=fromDateTime, to_time=toDateTime)
    return results



@app.get('/output/modelName={modelName}/modelVersion={modelVersion}/series={series}/location={location}/fromDateTime={fromDateTime}/toDateTime={toDateTime}')
async def get_output(modelName: str, modelVersion: str, series: str, location: str, fromDateTime: str, 
                     toDateTime: str, datum: str = None, interval: int = None):
    """
    Retrieves output series object

    Args:
        - `modelName` (string): The name of the model (e.g. "test AI")
        - `modelVersion` (string): The version of the model (e.g. "1.0.0")
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
    
    if interval is not None:
        interval = timedelta(seconds=interval)
    
    requestDescription = SemaphoreSeriesDescription(
        modelName, 
        modelVersion, 
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
    responseSeries = provider.request_output('SPECIFIC', semaphoreSeriesDescription=requestDescription, timeDescription=timeDescription)

    return responseSeries


@app.get("/health")
def health_check():
    return {"status": "up"}