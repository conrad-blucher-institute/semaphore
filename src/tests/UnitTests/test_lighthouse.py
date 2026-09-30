# -*- coding: utf-8 -*-
# test_lighthouse.py
#----------------------------------
# Created By: CJ Quintero
# Created On: 09/30/2026

#----------------------------------
""" 
This file provides unit tests for lighthouse

docker exec semaphore-core python3 -m pytest -s  ./src/tests/UnitTests/test_lighthouse.py
""" 
#----------------------------------
import json
from pathlib import Path
from unittest.mock import Mock
from datetime import datetime, timedelta, timezone

from DataClasses import TimeDescription, SeriesDescription
from src.DataIngestion.DI_Classes.LIGHTHOUSE import LIGHTHOUSE

FILE = Path(__file__).parent / "data" / "lighthouse_missing_values_response.json"
MISSING_VALUES = [None, '', '   ', 'nan', 'NaN', 'null', 'NULL', 'none', 'None']

class TestLighthouse():
    """Unit tests for lighthouse"""


    def test_missing_values(self):
        """
        tests that timestamps with missing data are not inserted into the output dataframe

        the file has 240 total data points and 33 timestamps are null

        docker exec semaphore-core python3 -m pytest -s  ./src/tests/UnitTests/test_lighthouse.py::TestLighthouse::test_missing_values
        """

        # the time description is set to match the saved response in the json file
        sd = SeriesDescription('LIGHTHOUSE', 'dWaterTmp', 'SouthBirdIsland')
        td = TimeDescription(
            datetime(2026, 9, 30, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 9, 30, 23, 54, tzinfo=timezone.utc),
            timedelta(minutes=6)
        )

        # instantiate lighthouse and mock storage
        lighthouse = LIGHTHOUSE()
        lighthouse.seriesStorage = Mock()
        lighthouse.seriesStorage.find_external_location_code.return_value = "013"

        # mock the return value of __api_request() to return the file instead
        with open(FILE) as f:
            data = json.load(f)
        lighthouse._LIGHTHOUSE__api_request = Mock(return_value=data)

        result = lighthouse.ingest_series(sd, td)

        assert result is not None
        # when None is casted to a string, it becomes 'None' as a string
        # and the class can already handle regular None and string 'None'
        assert not result.dataFrame['dataValue'].str.lower().isin(MISSING_VALUES).any()
        assert len(result.dataFrame) == 207
