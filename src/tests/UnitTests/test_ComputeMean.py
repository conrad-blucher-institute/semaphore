# -*- coding: utf-8 -*-
# test_ComputeMean.py
# -------------------------------
# Created By: Anointiyae Beasley
# Created Date: 08/19/2026
# -------------------------------

"""Test the ComputeMean post-processing class.

Run:
    docker exec semaphore-core python3 -m pytest -s src/tests/UnitTests/test_ComputeMean.py
"""

import sys
from datetime import datetime, timedelta, timezone
from math import isclose

import pandas as pd

sys.path.append("/app/src")

from src.DataClasses import (
    Series,
    SeriesDescription,
    TimeDescription,
    get_input_dataFrame,
)
from src.ModelExecution.dspecParser import PostProcessCall
from src.PostProcessing.IPostProcessing import post_processing_factory


START = datetime(2024, 1, 1, tzinfo=timezone.utc)

TIME_DESCRIPTION = TimeDescription(
    START,
    START + timedelta(hours=2),
    timedelta(hours=1),
)


def build_series_obj(data: list[object],sentinel_value: object,) -> Series:
    """Build one station Series with hourly timestamps."""
    test_series = Series(SeriesDescription("Test", "water-temp", "Test",),TIME_DESCRIPTION,)

    data_frame = get_input_dataFrame()

    for index, value in enumerate(data):
        verification_time = (
            START + timedelta(hours=index)
        )

        data_frame.loc[index] = [
            str(value),
            "degrees_C",
            verification_time,
            START,
            None,
            None,
        ]

    test_series.dataFrame = data_frame
    test_series.sentinelValue = sentinel_value

    return test_series


def test_compute_mean_for_timestamp():
    """Calculate one timestamp's mean and remove an outlier."""
    compute_mean = post_processing_factory(
        "ComputeMean"
    )

    station_values = pd.Series(
        [10.0, 12.0, 50.0]
    )

    result = compute_mean.compute_mean_for_timestamp(
        station_values,
        drop_outliers=True,
        threshold=3.5,
    )

    # Median: 12
    # Deviations: 2, 0, and 38
    # 50 is removed, so the mean is (10 + 12) / 2.
    assert isclose(result, 11.0, abs_tol=1e-5)


def test_get_station_values_replaces_sentinel():
    """Replace one station's sentinel with pandas NaN."""
    compute_mean = post_processing_factory(
        "ComputeMean"
    )

    station_series = build_series_obj(
        ["10", "1000", "14"],
        sentinel_value=1000,
    )

    result = compute_mean.get_station_values(
        "station-one",
        station_series,
    )

    assert result.name == "station-one"
    assert result.iloc[0] == 10
    assert pd.isna(result.iloc[1])
    assert result.iloc[2] == 14

    assert result.index.tolist() == [
        START,
        START + timedelta(hours=1),
        START + timedelta(hours=2),
    ]


def test_post_process_data():
    """Combine multiple station series into one mean series."""
    preprocessed_data = {
        "station-one": build_series_obj(
            ["10", "10", "10"],
            sentinel_value=1000,
        ),
        "station-two": build_series_obj(
            ["12", "1000", "12"],
            sentinel_value=1000,
        ),
        "station-three": build_series_obj(
            ["14", "14", "50"],
            sentinel_value="missing",
        ),
    }

    post_process_call = PostProcessCall()
    post_process_call.call = "ComputeMean"
    post_process_call.args = {
        "target_inKeys": [
            "station-one",
            "station-two",
            "station-three",
        ],
        "dropOutlierValues": True,
        "thresholdDeviationFromMedian": 3.5,
        "outKey": "combined-water-temp",
    }

    compute_mean = post_processing_factory(
        post_process_call.call
    )

    result = compute_mean.post_process_data(
        preprocessed_data,
        post_process_call,
    )

    output_series = result["combined-water-temp"]
    output_df = output_series.dataFrame

    actual_values = (
        output_df["dataValue"]
        .astype(float)
        .tolist()
    )

    expected_values = [
        12.0,  # Mean of 10, 12, and 14
        12.0,  # Sentinel removed; mean of 10 and 14
        11.0,  # Outlier 50 removed; mean of 10 and 12
    ]

    assert len(actual_values) == 3

    for actual, expected in zip(
        actual_values,
        expected_values,
    ):
        assert isclose(
            actual,
            expected,
            abs_tol=1e-5,
        )

    assert output_df["timeVerified"].tolist() == [
        START,
        START + timedelta(hours=1),
        START + timedelta(hours=2),
    ]

    assert output_df["dataUnit"].tolist() == [
        "degrees_C",
        "degrees_C",
        "degrees_C",
    ]

    assert output_series.sentinelValue == 1000