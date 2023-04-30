import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from  DataClasses import *


class DataManager():

    def make_request(self, request: Request) -> Response:
        