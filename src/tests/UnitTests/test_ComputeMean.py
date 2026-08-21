# -*- coding: utf-8 -*-
# test_ComputeMean.py
# -------------------------------
# Created By: Anointiyae Beasley
# Created Date: 08/19/2026
# -------------------------------

"""Tests for the ComputeMean post-processing class.

Run:
    docker exec semaphore-core python3 -m pytest -s \
        src/tests/UnitTests/test_ComputeMean.py
"""

import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

sys.path.append("/app/src")

from src.DataClasses import (
    Series,
    SeriesDescription,
    TimeDescription,
    get_input_dataFrame,
)
from src.ModelExecution.dspecParser import PostProcessCall
from src.PostProcessing.IPostProcessing import (
    post_processing_factory,
)


START = datetime(
    2024,
    1,
    1,
    tzinfo=timezone.utc,
)

EXPECTED_TIMESTAMPS = [
    START,
    START + timedelta(hours=1),
    START + timedelta(hours=2),
]

TIME_DESCRIPTION = TimeDescription(
    START,
    START + timedelta(hours=2),
    timedelta(hours=1),
)


@pytest.fixture
def compute_mean():
    """Return an instance of the ComputeMean class."""
    return post_processing_factory("ComputeMean")


def build_series_obj(
    data: list[object],
    sentinel_value: int | str,
) -> Series:
    """Build one station Series with hourly data."""
    series = Series(
        SeriesDescription(
            "Test",
            "water-temp",
            "Test",
        ),
        TIME_DESCRIPTION,
    )

    data_frame = get_input_dataFrame()

    for index, value in enumerate(data):
        data_frame.loc[index] = [
            str(value),
            "degrees_C",
            EXPECTED_TIMESTAMPS[index],
            START,
            None,
            None,
        ]

    series.dataFrame = data_frame
    series.sentinelValue = sentinel_value

    return series


@pytest.mark.parametrize(
    (
        "station_values",
        "drop_outliers",
        "threshold",
        "expected_mean",
    ),
    [
        (
            [10.0, 12.0, 14.0],
            False,
            None,
            12.0,
        ),
        (
            [10.0, 12.0, 50.0],
            True,
            3.5,
            11.0,
        ),
        (
            [10.0, float("nan"), 14.0],
            False,
            None,
            12.0,
        ),
    ],
)
def test_compute_mean_for_timestamp(
    compute_mean,
    station_values,
    drop_outliers,
    threshold,
    expected_mean,
):
    """
    Calculate the mean for one timestamp.

    This verifies that ComputeMean can calculate a regular mean,
    remove an outlier, and ignore NaN values.
    """
    values = pd.Series(station_values)

    actual_mean = (
        compute_mean.compute_mean_for_timestamp(
            values,
            drop_outliers=drop_outliers,
            threshold=threshold,
        )
    )

    assert actual_mean == pytest.approx(
        expected_mean
    )


def test_get_station_values_replaces_sentinel(
    compute_mean,
):
    """
    Convert station values to numeric and replace the sentinel with NaN.
    """
    station_series = build_series_obj(
        ["10", "1000", "14"],
        sentinel_value=1000,
    )

    result = compute_mean.get_station_values(
        "station-one",
        station_series,
    )

    assert result.name == "station-one"

    assert result.index.tolist() == (
        EXPECTED_TIMESTAMPS
    )

    assert result.iloc[0] == pytest.approx(10.0)
    assert pd.isna(result.iloc[1])
    assert result.iloc[2] == pytest.approx(14.0)


@pytest.mark.parametrize(
    (
        "drop_outliers",
        "expected_values",
    ),
    [
        (
            True,
            [12.0, 12.0, 11.0],
        ),
        (
            False,
            [12.0, 12.0, 24.0],
        ),
    ],
    ids=[
        "with-outlier-removal",
        "without-outlier-removal",
    ],
)
def test_post_process_data_combines_multiple_series(
    compute_mean,
    drop_outliers,
    expected_values,
):
    """
    Combine multiple station series and calculate a mean per timestamp.

    The test runs once with outlier removal and once without it.
    """
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
        "dropOutlierValues": drop_outliers,
        "outKey": "combined-water-temp",
    }

    if drop_outliers:
        post_process_call.args[
            "thresholdDeviationFromMedian"
        ] = 3.5

    result = compute_mean.post_process_data(
        preprocessed_data,
        post_process_call,
    )

    # The original series should remain in the dictionary.
    assert "station-one" in result
    assert "station-two" in result
    assert "station-three" in result

    # The combined series should be added to the dictionary.
    assert "combined-water-temp" in result

    output_series = result[
        "combined-water-temp"
    ]
    output_df = output_series.dataFrame

    actual_values = (
        output_df["dataValue"]
        .astype(float)
        .tolist()
    )

    assert actual_values == pytest.approx(
        expected_values
    )

    assert output_df[
        "timeVerified"
    ].tolist() == EXPECTED_TIMESTAMPS

    assert output_df["dataUnit"].tolist() == [
        "degrees_C",
        "degrees_C",
        "degrees_C",
    ]

    assert (
        output_series.description.dataSeries
        == "combined-water-temp"
    )

    assert (
        output_series.sentinelValue
        == preprocessed_data[
            "station-one"
        ].sentinelValue
    )