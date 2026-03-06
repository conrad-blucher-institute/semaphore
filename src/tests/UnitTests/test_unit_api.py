# -*- coding: utf-8 -*-
# test_unit_api.py
# -------------------------------
# Created By : Christian Quintero
# Created Date: 03/02/2026
# version 1.0
# -------------------------------
"""
This file tests that the api can correctly serialize ndarrays in the dataValue column of a data frame correctly

docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_unit_api.py
"""
import numpy as np
import pandas as pd
import pytest
from datetime import datetime
from DataClasses import Series, SemaphoreSeriesDescription
from src.API.apiDriver import serialize_output_series

@pytest.mark.parametrize(
    "data_array",
    [
        # Test case 1: shape (3, 5, 2)
        np.array([
            [
                [1.0, 2.0],
                [3.0, 4.0],
                [5.0, 6.0],
                [7.0, 8.0],
                [9.0, 10.0]
            ],
            [
                [11.0, 12.0],
                [13.0, 14.0],
                [15.0, 16.0],
                [17.0, 18.0],
                [19.0, 20.0]
            ],
            [
                [21.0, 22.0],
                [23.0, 24.0],
                [25.0, 26.0],
                [27.0, 28.0],
                [29.0, 30.0]
            ]
        ]),

        # Test case 2: None value
        None,

        # Test case 3: shape (1, 1, 1)
        np.array([
            [
                [42.0]
            ]
        ]),

        # Test case 4: shape (3, 5, 4)
        np.array([
            [
                [1.0, 2.0, 3.0, 4.0],
                [5.0, 6.0, 7.0, 8.0],
                [9.0, 10.0, 11.0, 12.0],
                [13.0, 14.0, 15.0, 16.0],
                [17.0, 18.0, 19.0, 20.0]
            ],
            [
                [21.0, 22.0, 23.0, 24.0],
                [25.0, 26.0, 27.0, 28.0],
                [29.0, 30.0, 31.0, 32.0],
                [33.0, 34.0, 35.0, 36.0],
                [37.0, 38.0, 39.0, 40.0]
            ],
            [
                [41.0, 42.0, 43.0, 44.0],
                [45.0, 46.0, 47.0, 48.0],
                [49.0, 50.0, 51.0, 52.0],
                [53.0, 54.0, 55.0, 56.0],
                [57.0, 58.0, 59.0, 60.0]
            ]
        ]),

        # Test case 5: shape (2, 3, 1)
        np.array([
            [
                [100.0],
                [200.0],
                [300.0]
            ],
            [
                [400.0],
                [500.0],
                [600.0]
            ]
        ]),

        # Test case 6: shape (1, 1, 0)
        np.array([
            [
                []
            ]
        ]),

        # Test case 7: shape (1, 2, 3)
        np.array([
            [
                [1.0, np.nan, 3.0],
                [4.0, 5.0, np.nan]
            ]
        ])
    ],
    ids=["3x5x2", "None", "1x1x1", "3x5x4", "2x3x1", "1x1x0", "nans"]
)
def test_serialize_output(data_array):
    """
    This test verifies that ndarrays in the dataValue column get converted to a list and serialized correctly.
    This test also checks that for a dataValue of None or containing NaNs, that the serialized output has dataValue set to None

    docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_unit_api.py::test_serialize_output -s
    """

    df = pd.DataFrame([
        {
            "dataValue": data_array,
            "dataUnit": "celsius",
            "timeGenerated": datetime(2026, 1, 1, 0, 0, 0),
            "leadTime": 367200
        }
    ])

    # Create a mock Series object
    desc = SemaphoreSeriesDescription("model", "1.0", "series", "location", "datum")
    series = Series(description=desc)
    series.dataFrame = df

    # Call the serializer
    result = serialize_output_series(series)

    # ensure the cases for None and an array with any nans are handled correctly
    if data_array is None or pd.isna(data_array).any():
        assert result['_Series__data'][0]['dataValue'] is None
    # ensure in normal cases that the ndarray in dataValue is converted to a list and serialized correctly
    else:
        assert result['_Series__data'][0]['dataValue'] == data_array.tolist()

    assert result['_Series__data'][0]['dataUnit'] == "celsius"
    assert result['_Series__data'][0]['leadTime'] == 367200
    assert result['_Series__data'][0]['timeGenerated'] == "2026-01-01T00:00:00"

def test_serialize_output_multiple_rows():
    """
    This test verifies that when a data frame has many rows, that each row's dataValue column
    gets converted to a list and serialized correctly.

    docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_unit_api.py::test_serialize_output_multiple_rows -s
    """

    # (3, 5, 2)
    arr1 = np.array([
        [
            [1.0, 2.0],
            [3.0, 4.0],
            [5.0, 6.0],
            [7.0, 8.0],
            [9.0, 10.0]
        ],
        [
            [11.0, 12.0],
            [13.0, 14.0],
            [15.0, 16.0],
            [17.0, 18.0],
            [19.0, 20.0]
        ],
        [
            [21.0, 22.0],
            [23.0, 24.0],
            [25.0, 26.0],
            [27.0, 28.0],
            [29.0, 30.0]
        ]
    ])

    # None
    arr2 = None

    # (1, 1, 1)
    arr3 = np.array([
        [
            [42.0]
        ]
    ])

    # (1, 1, 0)
    arr4 = np.array([
        [
            [
                
            ]
        ]
    ])

    arr5 = np.array([
        [
            [1.0, np.nan, 3.0],
            [4.0, 5.0, np.nan]
        ]
    ])

    tg = datetime(2026, 1, 1, 0, 0, 0)
    lt = 367200
    unit = "celsius"

    df = pd.DataFrame([
        {
            'dataValue': arr1,
            'dataUnit': unit,
            'timeGenerated': tg,
            'leadTime': lt
        },
        {
            'dataValue': arr2,
            'dataUnit': unit,
            'timeGenerated': tg,
            'leadTime': lt
        },
        {
            'dataValue': arr3,
            'dataUnit': unit,
            'timeGenerated': tg,
            'leadTime': lt
        },
        {
            'dataValue': arr4,
            'dataUnit': unit,
            'timeGenerated': tg,
            'leadTime': lt
        },
        {
            'dataValue': arr5,
            'dataUnit': unit,
            'timeGenerated': tg,
            'leadTime': lt
        }
    ])

    desc = SemaphoreSeriesDescription("model", "1.0", "series", "location", "datum")
    series = Series(description=desc)
    series.dataFrame = df

    # call the serializer
    result = serialize_output_series(series)

    for idx, row in df.iterrows():
        # ensure the cases for None and an array with any nans are handled correctly
        if row['dataValue'] is None or pd.isna(row['dataValue']).any():
            assert result['_Series__data'][idx]['dataValue'] is None
        # ensure in normal cases that the ndarray in dataValue is converted to a list and serialized correctly
        # and that it matches the original dataValue in the data frame
        else:
            assert result['_Series__data'][idx]['dataValue'] == df.iloc[idx]['dataValue'].tolist()

        assert result['_Series__data'][idx]['dataUnit'] == "celsius"
        assert result['_Series__data'][idx]['leadTime'] == 367200
        assert result['_Series__data'][idx]['timeGenerated'] == "2026-01-01T00:00:00"

