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
import pytest
from pathlib import Path
from datetime import datetime

from src.DataClasses import get_input_dataFrame
from src.DataIngestion.DI_Classes.SEMAPHORE import SEMAPHORE

import pandas as pd
import numpy as np

FILE = Path(__file__).parent / "data" / "semaphore_response.json"
MISSING_VALUES = [None, 'None', 'none', 'NONE', np.nan, 'nan', 'NaN', 'NAN', '', 'NULL', 'null', 'Null']

@pytest.fixture
def semaphore():
    return SEMAPHORE.__new__(SEMAPHORE)  # skip __init__ / storage connection

class TestSemaphoreIngestion():
    """testing suite for the SEMAPHORE ingestion class"""

    def test_missing_values_with_response(self, semaphore):
        """
        tests that the ingestion class removes verified timestamps with missing values
        using a real response from the Semaphore API

        docker exec semaphore-core python3 -m pytest -s ./src/tests/UnitTests/test_SemaphoreIngestion.py::TestSemaphoreIngestion::test_missing_values_with_response
        """
        with open(FILE) as f:
            data = json.load(f)

        df = pd.DataFrame(data['_Series__data'])

        result = semaphore._SEMAPHORE__filter_input_df(df)

        assert result is not None
        assert result['dataValue'].notna().all()
        assert not result['dataValue'].isin(MISSING_VALUES).any()
        # there is 617 total rows, 69 rows with missing values
        # so there should only be 548 rows left
        assert len(result) == 548

    @pytest.mark.parametrize(
        "data, timestamps, expected_timestamps, expected_len",
        [
            # test that the filtering function removes timestamps with missing values
            (
                # 10 timestamps with 3 missing values
                [1, 2, np.nan, '', None, 6, 7, 8, 9, 10],
                pd.date_range(datetime(2026, 1, 1, 0), datetime(2026, 1, 1, 9), freq='1h'),
                [
                    # the 3 missing value rows should be removed so their
                    # timestamps should not be expected in the result
                    datetime(2026, 1, 1, 0),
                    datetime(2026, 1, 1, 1),
                    datetime(2026, 1, 1, 5),
                    datetime(2026, 1, 1, 6),
                    datetime(2026, 1, 1, 7),
                    datetime(2026, 1, 1, 8),
                    datetime(2026, 1, 1, 9)
                ],
                7
            ),
            (
                # all empty values so there should be no timestamps in the result
                [None, None, None],
                pd.date_range(datetime(2026, 1, 1, 0), datetime(2026, 1, 1, 2), freq='1h'),
                [],
                0
            ),
            (
                [1, '', '', '', '', '', '', '', '', '', '', ''],
                pd.date_range(datetime(2026, 1, 1, 0), datetime(2026, 1, 1, 11), freq='1h'),
                [
                    datetime(2026, 1, 1, 0)
                ],
                1
            ),
            (
                # no filtering
                [1, 2, 3, 4, 5],
                pd.date_range(datetime(2026, 1, 1, 0), datetime(2026, 1, 1, 4), freq='1h'),
                [
                    # all timestamps should remain
                    datetime(2026, 1, 1, 0),
                    datetime(2026, 1, 1, 1),
                    datetime(2026, 1, 1, 2),
                    datetime(2026, 1, 1, 3),
                    datetime(2026, 1, 1, 4)
                ],
                5
            ),
            # test that all timestamps remain and that string values are not filtered out
            (
                ['1', '2', '3', '4', '5'],
                pd.date_range(datetime(2026, 1, 1, 0), datetime(2026, 1, 1, 4), freq='1h'),
                [
                    datetime(2026, 1, 1, 0),
                    datetime(2026, 1, 1, 1),
                    datetime(2026, 1, 1, 2),
                    datetime(2026, 1, 1, 3),
                    datetime(2026, 1, 1, 4)
                ],
                5
            ),
            # test the filtering works with string data values, and removes None, nan, and empty strings
            (
                ['1', '2', '', '4', '5', '', '', '', np.nan, np.nan, None, None, ''],
                pd.date_range(datetime(2026, 1, 1, 0), datetime(2026, 1, 1, 12), freq='1h'),
                [
                    datetime(2026, 1, 1, 0),
                    datetime(2026, 1, 1, 1),
                    datetime(2026, 1, 1, 3),
                    datetime(2026, 1, 1, 4)
                ],
                4
            ),
            # test the filtering works with stringified missing values
            (
                ['1', '2', 'None', '4', '5', 'none', 'nan', 'NaN', 'NAN', 'NULL', 'null', 'Null', 'None'],
                pd.date_range(datetime(2026, 1, 1, 0), datetime(2026, 1, 1, 12), freq='1h'),
                [
                    datetime(2026, 1, 1, 0),
                    datetime(2026, 1, 1, 1),
                    datetime(2026, 1, 1, 3),
                    datetime(2026, 1, 1, 4)
                ],
                4
            )
        ]
    )
    def test_missing_values(self, data, timestamps, expected_timestamps, expected_len, semaphore):
        """
        tests that the ingestion class removes verified timestamps with missing values

        docker exec semaphore-core python3 -m pytest -s ./src/tests/UnitTests/test_SemaphoreIngestion.py::TestSemaphoreIngestion::test_missing_values
        """
        df = get_input_dataFrame()
        df['timeVerified'] = timestamps
        df['dataValue'] = data

        result = semaphore._SEMAPHORE__filter_input_df(df)

        assert not result['dataValue'].isna().any(), "Filtered DataFrame should not contain NaN or None values"

        # Check that there are no leftover missing values in the filtered DataFrame
        # using the MISSING_VALUES list to check for any remaining missing values
        leftover = result[result['dataValue'].isin(MISSING_VALUES)]
        assert leftover.empty, f"Filtered DataFrame contains leftover missing values: {leftover}"

        assert len(result) == expected_len, f"Filtered DataFrame should have {expected_len} rows, but got {len(result)}"
        assert expected_timestamps == list(result['timeVerified']), f"Filtered timestamps do not match expected timestamps. Expected: {expected_timestamps}, Got: {list(result['timeVerified'])}"


    def test_filtering_doesnt_affect_other_columns(self, semaphore):
        """
        tests that the filtering function does not remove or modify values in other columns

        docker exec semaphore-core python3 -m pytest -s  ./src/tests/UnitTests/test_SemaphoreIngestion.py::TestSemaphoreIngestion::test_filtering_doesnt_affect_other_columns
        """
        df = get_input_dataFrame()
        df['timeVerified'] = pd.date_range(datetime(2026, 1, 1, 0), datetime(2026, 1, 1, 4), freq='1h')
        df['timeGenerated'] = pd.date_range(datetime(2026, 1, 1, 0), datetime(2026, 1, 1, 4), freq='1h')
        df['dataValue'] = ['1', 'None', None, '4', '5']
        df['dataUnit'] = ['Unit1', 'Unit2', 'Unit3', 'Unit4', 'Unit5']
        df['latitude'] = ['Lat1', 'Lat2', 'Lat3', 'Lat4', 'Lat5']
        df['longitude'] = ['Lon1', 'Lon2', 'Lon3', 'Lon4', 'Lon5']

        expected_df = get_input_dataFrame()
        expected_df['timeVerified'] = [
            datetime(2026, 1, 1, 0),
            datetime(2026, 1, 1, 3),
            datetime(2026, 1, 1, 4)
        ]
        expected_df['timeGenerated'] = [
            datetime(2026, 1, 1, 0),
            datetime(2026, 1, 1, 3),
            datetime(2026, 1, 1, 4)
        ]
        expected_df['dataValue'] = ['1', '4', '5']
        expected_df['dataUnit'] = ['Unit1', 'Unit4', 'Unit5']
        expected_df['latitude'] = ['Lat1', 'Lat4', 'Lat5']
        expected_df['longitude'] = ['Lon1', 'Lon4', 'Lon5']

        result = semaphore._SEMAPHORE__filter_input_df(df)

        # Check that the other columns remain unchanged for the rows that are kept
        pd.testing.assert_frame_equal(result, expected_df)
