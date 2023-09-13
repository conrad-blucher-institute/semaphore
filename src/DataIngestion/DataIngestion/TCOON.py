import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from DataIngestion.IDataIngestion import IDataIngestion
from DataClasses import SeriesDescription, Series

class TCOON(IDataIngestion):

    def ingest_series(self, seriesDescription: SeriesDescription) -> Series | None:
        pass
     