# -*- coding: utf-8 -*-
# test_NDBC.py
#----------------------------------
# Created By: CJ Quintero
# Created On: 09/30/2026

#----------------------------------
""" 
This file provides unit tests for NDBC

NOTE: a few values were inserted into the WVHT column since this
column in the response is all missing and is set to MM.

docker exec semaphore-core python3 -m pytest -s  ./src/tests/UnitTests/test_NDBC.py
""" 
#----------------------------------
from pathlib import Path
from unittest.mock import Mock
from datetime import datetime, timedelta, timezone

from DataClasses import TimeDescription, SeriesDescription
from src.DataIngestion.DI_Classes.NDBC import NDBC

FILE = Path(__file__).parent / "data" / "ndbc_response.txt"
MISSING_VALUES = [None, '', '   ', 'nan', 'NaN', 'null', 'NULL', 'none', 'None', 'mm', 'MM']

class TestNDBC():

    def test_missing_values(self):
        """
        tests that missing values aren't inserted into the input df

        NOTE: a few values were inserted into the WVHT column since this
        column in the response is all missing and is set to MM.

        docker exec semaphore-core python3 -m pytest -s  ./src/tests/UnitTests/test_NDBC.py::TestNDBC::test_missing_values
        """

        sd = SeriesDescription('NDBC', 'WVHT', 'RandomLocation')
        td = TimeDescription(
            datetime(2026, 9, 22, 7, 30, tzinfo=timezone.utc),
            datetime(2026, 9, 30, 20, 30, tzinfo=timezone.utc),
            timedelta(hours=1)
        )

        ndbc = NDBC()
        ndbc.seriesStorage = Mock()
        ndbc.seriesStorage.find_external_location_code.return_value = "32ST0"

        # mock the return value of __fetch() to return the file instead
        with open(FILE) as f:
            data = f.read()
        ndbc._NDBC__fetch = Mock(return_value=data)

        result = ndbc.ingest_series(sd, td)

        assert result is not None
        assert not result.dataFrame['dataValue'].str.lower().isin(MISSING_VALUES).any()
        assert len(result.dataFrame) == 5

        expected_times = [
            datetime(2026, 9, 30, 20, 30, tzinfo=timezone.utc),
            datetime(2026, 9, 30, 14, 30, tzinfo=timezone.utc),
            datetime(2026, 9, 28, 22, 30, tzinfo=timezone.utc),
            datetime(2026, 9, 28, 21, 30, tzinfo=timezone.utc),
            datetime(2026, 9, 28, 9, 30, tzinfo=timezone.utc),
        ]
        assert list(result.dataFrame['timeVerified']) == expected_times
        assert list(result.dataFrame['dataValue']) == ['1.0', '2.0', '3.0', '4.0', '5.0']
