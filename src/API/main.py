from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from API.datetime_handler import parse_date
from DataClasses import SeriesDescription, SemaphoreSeriesDescription
from SeriesProvider.SeriesProvider import SeriesProvider

load_dotenv()

app = FastAPI()

@app.get('/')
def read_main():
    return {'message': 'Hello World'}


@app.get('/input/source={source}/series={series}/classification={classification}/location={location}/ \
         unit={unit}/interval={interval}/fromDateTime={fromDateTime}/toDateTime={toDateTime}')
async def get_input(source: str, series: str, classification: str, location: str, unit: str, interval: int, 
                    fromDateTime: str, toDateTime: str, datum: str = None):
    """
    Retrieves input series object

    Args:

        - `source` (string): The data's source (e.g. "noaaT&C")

        - `series` (string): The series name (e.g. "dXWnCmp")

        - `classification` (string): Actual/prediction/output identifier ("actual" or "prediction")

        - `location` (string): The location of the data (e.g. "packChan")

        - `unit` (string): The unit (e.g. 'meter')
            
        - `interval` (int): The time step separating the data points in seconds (e.g. 3600 for hourly)

        - `fromDateTime` (string): "YYYYMMDDHH" Date to start at

        - `toDateTime` (string): "YYYYMMDDHH" Date to end at

        - `datum` (string): Optional. Defaults to None

    Returns:
        Series: A series object holding either the requested data or an error message with the incomplete data. (src/DataManagement/DataClasses.py)

    Raises:
        HTTPException: If the series is not found.
    """
    try:
        fromDateTime = parse_date(fromDateTime)
        toDateTime = parse_date(toDateTime)
    except TypeError as e:
        raise HTTPException(status_code=404, detail=f'{e}')
    except ValueError as e:
        raise HTTPException(status_code=404, detail=f'{e}')
    
    requestDesc = SeriesDescription(
        source, 
        series,
        classification,
        location, 
        unit, 
        interval,
        fromDateTime, 
        toDateTime,
        datum
    )

    provider = SeriesProvider()
    responseSeries = provider.make_request(requestDesc)

    return responseSeries


@app.get('/output/ModelName={ModelName}/ModelVersion={ModelVersion}/series={series}/location={location}/ \
         interval={interval}/fromDateTime={fromDateTime}/toDateTime={toDateTime}/leadTime={leadTime}')
async def get_output(ModelName: str, ModelVersion: str, series: str, location: str, interval: int, 
                    fromDateTime: str, toDateTime: str, leadTime: float, datum: str = None):
    """
    Retrieves output series object

    Args:

        - `ModelName` (string): The name of the model (e.g. "test AI")

        - `ModelVersion` (string): The version of the model (e.g. "1.0.0")

        - `series` (string): The series name (e.g. "waterHeight")

        - `location` (string): The location of the data (e.g. "packChan")

        - `interval` (int): The time step separating the data points in seconds (e.g. 3600 for hourly)

        - `fromDateTime` (string): "YYYYMMDDHH" Date to start at

        - `toDateTime` (string): "YYYYMMDDHH" Date to end at

        - `leadTime` (float): The time in hours between the generated time and verification time (e.g. 24 hours)

        - `datum` (string): Optional. Defaults to None

    Returns:
        Series: A series object holding either the requested data or an error message with the incomplete data. (src/DataManagement/DataClasses.py)

    Raises:
        HTTPException: If the series is not found.
    """ 
    try:
        fromDateTime = parse_date(fromDateTime)
        toDateTime = parse_date(toDateTime)
    except TypeError as e:
        raise HTTPException(status_code=404, detail=f'{e}')
    except ValueError as e:
        raise HTTPException(status_code=404, detail=f'{e}')
    
    requestDesc = SemaphoreSeriesDescription(
        ModelName, 
        ModelVersion, 
        series, 
        location,
        interval,
        leadTime,
        datum
    )

    requestDesc.fromDateTime = fromDateTime
    requestDesc.toDateTime = toDateTime

    provider = SeriesProvider()
    responseSeries = provider.make_output_request(requestDesc)

    return responseSeries