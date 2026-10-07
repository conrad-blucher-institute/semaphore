# -*- coding: utf-8 -*-
# test_statistics.py
#----------------------------------
# Created By : Christian Quintero
# Created Date: 04/20/2026
# version 1.0
#----------------------------------
"""
This file tests the Statistics class in statistics.py

docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_statistics.py
""" 
#----------------------------------
import pytest
import numpy as np
from src.ModelExecution.statistics import Statistics


class TestStatistics():

    @pytest.mark.parametrize("data, expected", 
    [
        # Basic test with a list of values
        (
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],

            {
                'p1': 1.09,
                'p5': 1.45,
                'p10': 1.9,
                'p25': 3.25,
                'p50': 5.5,
                'p75': 7.75,
                'p90': 9.1,
                'p95': 9.55,
                'p99': 9.91,
                'min': 1,
                'max': 10,
                'mean': 5.5,
                'std_dev': 2.872281323269
            }
        ),

        # Test with a 3D numpy array
        # ensure multidimensional arrays do not affect results (they should be flattened to 1D)
        (
            np.array([
                [
                    [1, 2],
                    [3, 4]
                ],
                [
                    [5, 6],
                    [7, 8]
                ],
                [
                    [9, 10],
                    [11, 12]
                ]
            ]),

            {
                'p1': 1.11,
                'p5': 1.55,
                'p10': 2.1,
                'p25': 3.75,
                'p50': 6.5,
                'p75': 9.25,
                'p90': 10.9,
                'p95': 11.45,
                'p99': 11.89,
                'min': 1,
                'max': 12,
                'mean': 6.5,
                'std_dev': 3.4520525295347
            }
        ),

        # Test with a 2D list
        (
            [
                [1, 2, 3, 4, 5],
                [6, 7, 8, 9, 10]
            ],

            {
                'p1': 1.09,
                'p5': 1.45,
                'p10': 1.9,
                'p25': 3.25,
                'p50': 5.5,
                'p75': 7.75,
                'p90': 9.1,
                'p95': 9.55,
                'p99': 9.91,
                'min': 1,
                'max': 10,
                'mean': 5.5,
                'std_dev': 2.872281323269
            }
        )
    ],
    ids = [
        "basic_list",
        "3d_ndarray",
        "2d_list",
    ])
    def test_compute_statistics(self, data, expected):
        statistics = Statistics()
        result = statistics.compute_statistics(data)

        # assert no extra keys are in the result
        assert len(result) == len(expected), f"Expected {len(expected)} keys in the result, but got {len(result)}"
        
        # assert all expected values are within 3 decimal places of the result
        for key in expected:
            # assert values are close to expected values
            assert np.isclose(result[key], expected[key], atol=1e-3), f"Expected {key} to be {expected[key]}, but got {result[key]}"
            # assert values are all regular python floats, not numpy floats
            assert isinstance(result[key], float), f"Expected {key} to be a float, but got {type(result[key])}"

    
    @pytest.mark.parametrize("data, expected_message",
    [
        # Test with an empty list
        (
            [],
            "Cannot compute statistics on an empty dataset."
        ),

        # Test with a list containing NaN values
        (
            [1, 2, np.nan, 4, 5],
            "Cannot compute statistics on a dataset containing NaN values."
        )
    ],
    ids = [
        "empty_list",
        "list_with_nan"
    ])
    def test_compute_statistics_invalid_input(self, data, expected_message):
        statistics = Statistics()
        with pytest.raises(ValueError) as exc_info:
            statistics.compute_statistics(data)
        
        assert str(exc_info.value) == expected_message
        
