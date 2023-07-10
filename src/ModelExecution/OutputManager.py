import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))
from DataManagement.DataClasses import Prediction, SaveRequest, Response
from DataManagement.DataManager import DataManager


from utility import log
from datetime import datetime

class OutputManager():

    def __init__(self) -> None:
        self.dataManager = DataManager()

    def output_method_map(self, method, prediction, AIName: str, generatedTime: datetime, leadTime: float, AIGeneratedVersion: str, location: str, datum: str = None, latitude: str = None, longitude: str = None):
        match method:
                case 'one_packed_float':
                    return self.__one_packed_float( prediction, AIName, generatedTime, leadTime, AIGeneratedVersion, location, datum, latitude, longitude)
                case _:
                    log(f'No output method found for {method}!')
                    return None
    
    def __one_packed_float(self, prediction, AIName: str, generatedTime: datetime, leadTime: float, AIGeneratedVersion: str, location: str, datum: str = None, latitude: str = None, longitude: str = None) -> Response:

        prediction = Prediction(prediction, 'float', leadTime, generatedTime)
        request = SaveRequest(AIName, AIGeneratedVersion, location, datum, latitude, longitude)
        return self.dataManager.make_SaveRequest(request)
