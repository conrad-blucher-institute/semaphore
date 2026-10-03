# -*- coding: utf-8 -*-
# test_SemaphoreIngestion.py
#----------------------------------
# Created By: CJ Quintero
# Created On: 10/03/2026

#----------------------------------
""" 
This file provides unit tests for the SEMAPHORE ingestion class

NOTE: The Semaphore ingestion class uses the stringified "None"
for missing values.

docker exec semaphore-core python3 -m pytest -s ./src/tests/UnitTests/test_SemaphoreIngestion.py
""" 
#----------------------------------
import json
from pathlib import Path
from unittest.mock import Mock

from src.DataIngestion.DI_Classes.SEMAPHORE import SEMAPHORE

from pandas import DataFrame

FILE = Path(__file__).parent / "data" / "semaphore_response.json"
MISSING_VALUES = ["None", "nan", ""]

class TestSemaphoreIngestion():

    def test_missing_values(self):
        """
        tests that the ingestion class removes verified timestamps with missing values

        docker exec semaphore-core python3 -m pytest -s ./src/tests/UnitTests/test_SemaphoreIngestion.py::TestSemaphoreIngestion::test_missing_values
        """
        semaphore = SEMAPHORE()
        semaphore.series_storage = Mock()

        with open(FILE) as f:
            data = json.load(f)

        df = DataFrame(data['_Series__data'])

        result = semaphore._SEMAPHORE__filter_input_df(df)

        assert result is not None
        assert result['dataValue'].notna().all()
        assert not result['dataValue'].isin(MISSING_VALUES).any()
        # there is 617 total rows, 69 rows with missing values
        # so there should only be 548 rows left
        assert len(result) == 548