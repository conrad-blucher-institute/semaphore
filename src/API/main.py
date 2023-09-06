from dotenv import load_dotenv
from fastapi import FastAPI, Response, HTTPException
from fastapi.responses import StreamingResponse

from datetime import datetime
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.pardir)))
sys.path.append(os.path.dirname(os.path.abspath(os.path.curdir)))

from ModelExecution.modelWrapper import ModelWrapper

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