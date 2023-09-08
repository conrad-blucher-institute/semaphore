from dotenv import load_dotenv
from fastapi import FastAPI, Response, HTTPException
from fastapi.responses import StreamingResponse

from datetime import datetime, date, time
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

'''
sys.path.append(os.path.dirname(os.path.abspath(os.path.pardir)))
sys.path.append(os.path.dirname(os.path.abspath(os.path.curdir)))
'''
from DataClasses import SeriesDescription
from ModelExecution.modelWrapper import ModelWrapper
from SeriesProvider.SeriesProvider import SeriesProvider

load_dotenv()

app = FastAPI()


@app.get('/')
def read_main():
    return {'message': 'Hello World'}


@app.get('/prediction')
async def get_prediction():
    """
    Retrieve prediction

    Args:

    Returns:

    Raises:
        HTTPException: If the item is not found.
    """
    now = datetime.now()

    MW = ModelWrapper('test_dspec.json')
    result = MW.make_and_save_prediction(now)

    print(result)
    print(type(result))
    print(result.data)
    print(type(result.data))
    print(result.data[0])
    print(type(result.data[0]))
    print(result.data[0].unit)

    #return {'message': f'{result.data}'}
    return result.data[0]


@app.get('/input/source={source}/series={series}/location={location}/unit={unit}/interval={interval}')
async def get_input(source: str, series: str, location: str, unit: str, interval: int, 
                    fromDateTime: datetime = None, toDateTime: datetime = None, datum: str = None):
    """
    Retrieve input

    Args:

    Returns:

    Raises:
        HTTPException: If the item is not found.
    """
    now = datetime.now()
    fromDateTime = datetime.combine(date(2023, 9, 4), time(11, 0))
    toDateTime = datetime.combine(date(2023, 9, 5), time(11, 0))
    
    requestDesc = SeriesDescription(
        source, 
        series, 
        location, 
        unit, 
        interval,
        fromDateTime, 
        toDateTime, 
    )

    provider = SeriesProvider()
    responseSeries = provider.make_request(requestDesc)

    print(responseSeries)
    print(responseSeries.data)

    return responseSeries