# -*- coding: utf-8 -*-
# test_ComputeMean.py
# -------------------------------
# Created By: Anointiyae Beasley
# Created Date: 08/19/2026
# -------------------------------

"""
Tests for the ComputeMean post-processing class.

Create input DataFrames
    ↓
Place them inside Series objects
    ↓
Place the Series in preprocessed_data
    ↓
Call post_process_data()
    ↓
Retrieve the newly created Series
    ↓
Assert that its DataFrame is correct

Run:
    docker exec semaphore-core python3 -m pytest -s \
src/tests/UnitTests/test_ComputeMean.py
"""

import sys
from datetime import datetime, timedelta, timezone

sys.path.append("/app/src")

from src.DataClasses import Series, SeriesDescription, TimeDescription, get_input_dataFrame
from src.ModelExecution.dspecParser import PostProcessCall
from src.PostProcessing.IPostProcessing import post_processing_factory

import pandas as pd
import numpy as np
import pytest

START = datetime(
    2024,
    1,
    1,
    tzinfo=timezone.utc
)

EXPECTED_TIMESTAMPS = [
    START,
    START + timedelta(hours=1),
    START + timedelta(hours=2)
]

TIME_DESCRIPTION = TimeDescription(
    START,
    START + timedelta(hours=2),
    timedelta(hours=1)
)

@pytest.fixture
def compute_mean():
    """Return an instance of the ComputeMean class."""
    return post_processing_factory("ComputeMean")


def build_series_obj(data: list[object], sentinel_value: int | str) -> Series:
    """Build one Series with hourly data."""

    series = Series(SeriesDescription("Test", "water-temp", "Test"), TIME_DESCRIPTION)

    data_frame = get_input_dataFrame()

    for index, value in enumerate(data):
        data_frame.loc[index] = [
            str(value),
            "degrees_C",
            EXPECTED_TIMESTAMPS[index],
            START,
            None,
            None
        ]

    series.dataFrame = data_frame
    series.sentinelValue = sentinel_value

    return series


@pytest.mark.parametrize(
    (
        "station_values",
        "drop_outliers",
        "threshold",
        "expected_mean"
    ),
    [
        (
            [10.0, 12.0, 14.0],
            False,
            None,
            12.0
        ),
        (
            [10.0, 12.0, 50.0],
            True,
            3.5,
            11.0
        ),
        (
            [10.0, float("nan"), 14.0],
            False,
            None,
            12.0
        )
    ]
)
def test_compute_mean(compute_mean, station_values, drop_outliers, threshold, expected_mean):
    """
    Calculate the mean for one timestamp.

    This verifies that ComputeMean can calculate a regular mean,
    remove an outlier, and ignore NaN values.
    """
    values = pd.Series(station_values)

    actual_mean = (
        compute_mean._compute_mean(
            values,
            drop_outliers=drop_outliers,
            threshold=threshold,
        )
    )

    assert actual_mean == pytest.approx(expected_mean)


def test_get_series_values_replaces_sentinel(compute_mean):
    """
    tests that the sentinel value in a series is replaced with NaN when calling _get_series_values
    """

    station_series = build_series_obj(["10", "1000", "14"], sentinel_value=1000)

    result = compute_mean._get_series_values(station_series)

    assert result.index.tolist() == (EXPECTED_TIMESTAMPS)
    assert result.iloc[0] == pytest.approx(10.0)
    assert pd.isna(result.iloc[1])
    assert result.iloc[2] == pytest.approx(14.0)


@pytest.mark.parametrize(
    (
        "drop_outliers",
        "expected_values"
    ),
    [
        (
            True,
            [12.0, 12.0, 11.0]
        ),
        (
            False,
            [12.0, 12.0, 24.0]
        )
    ],
    ids=[
        "with-outlier-removal",
        "without-outlier-removal"
    ]
)
def test_post_process_data_combines_multiple_series(compute_mean, drop_outliers, expected_values):
    """
    Tests that the overall post_process_data function works as intended

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
        )
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
        post_process_call
    )

    # The original series should remain in the dictionary.
    assert "station-one" in result
    assert "station-two" in result
    assert "station-three" in result

    # The combined series should be added to the dictionary.
    assert "combined-water-temp" in result

    # get the resulting df and the actual values after the mean has been computed
    output_series = result["combined-water-temp"]
    output_df = output_series.dataFrame
    actual_values = (output_df["dataValue"].astype(float).tolist())

    assert actual_values == pytest.approx(expected_values)

    assert output_df["timeVerified"].tolist() == EXPECTED_TIMESTAMPS

    assert output_df["dataUnit"].isna().all()

    assert (output_series.description.dataSeries == "combined-water-temp")

    # The computed series should not inherit an input series' sentinel value
    assert output_series.sentinelValue is None


def test_get_series_values_rejects_non_numeric_non_sentinel(compute_mean):
    """Tests a nonnumeric value that is not the sentinel should raise an error."""

    station_series = build_series_obj(
        ["10", "invalid", "14"],
        sentinel_value=1000,
    )

    # should throw an error since .to_numeric() should fail when converting "invalid" to a float
    with pytest.raises(ValueError):
        compute_mean._get_series_values(station_series)


def test_get_series_values_replaces_string_sentinel(compute_mean):
    """
    tests that a string sentinel value is replaced with NaN when calling _get_series_values

    EX: if we use "missing" as the sentinel value in a dspec, _get_series_values
    should replace "missing" with NaN.
    """
    station_series = build_series_obj(
        ["10", "missing", "14"],
        sentinel_value="missing",
    )

    result = compute_mean._get_series_values(station_series)

    assert result.iloc[0] == pytest.approx(10.0)
    assert pd.isna(result.iloc[1])
    assert result.iloc[2] == pytest.approx(14.0)


def test_post_process_data_uses_union_of_timestamps(compute_mean):
        """
        Tests that the output series does not drop any timestamps present in any of the input series.

        EX: a timestamp that only appears in 1 series should still be present in the output
        """
        station_one = build_series_obj(
            ["10", "12", "14"],
            sentinel_value=1000
        )

        station_two = build_series_obj(
            ["20", "22", "24"],
            sentinel_value=1000
        )

        # Remove station two's first timestamp.
        station_two.dataFrame = (
            station_two.dataFrame.iloc[1:]
            .reset_index(drop=True)
        )

        preprocessed_data = {
            "station-one": station_one,
            "station-two": station_two
        }

        post_process_call = PostProcessCall()
        post_process_call.call = "ComputeMean"
        post_process_call.args = {
            "target_inKeys": [
                "station-one",
                "station-two",
            ],
            "dropOutlierValues": False,
            "outKey": "ESB-combined-water-temp"
        }

        result = compute_mean.post_process_data(preprocessed_data, post_process_call)

        output_df = result["ESB-combined-water-temp"].dataFrame

        assert output_df["timeVerified"].tolist() == (EXPECTED_TIMESTAMPS)

        assert output_df["dataValue"].astype(float).tolist() == (pytest.approx([10.0, 17.0, 19.0]))


def test_post_process_data_raises_for_missing_input_key(compute_mean):
        """
        Asserts that ComputeMean will raise a KeyError if a target_inKey is not present in the
        preprocessed_data repository.
        """

        preprocessed_data = {
            "station-one": build_series_obj(
                ["10", "12", "14"],
                sentinel_value=1000,
            ),
        }

        post_process_call = PostProcessCall()
        post_process_call.call = "ComputeMean"
        post_process_call.args = {
            "target_inKeys": [
                "station-one",
                "missing-station",
            ],
            "dropOutlierValues": False,
            "outKey": "ESB-combined-water-temp",
        }

        # assert the proper exception was raised
        with pytest.raises(KeyError, match="ComputeMean could not find these target series:"):
            compute_mean.post_process_data(preprocessed_data, post_process_call)


def test_post_process_data_raises_for_empty_target_keys(compute_mean):
        """
        tests that ComputeMean raises an exception if the target_inKeys list is empty
        """

        post_process_call = PostProcessCall()
        post_process_call.call = "ComputeMean"
        post_process_call.args = {
            "target_inKeys": [],
            "dropOutlierValues": False,
            "outKey": "ESB-combined-water-temp"
        }

        with pytest.raises(ValueError, match="ComputeMean requires at least one target input series."):
            compute_mean.post_process_data({}, post_process_call)


@pytest.mark.parametrize(
    "threshold",
    [
        None,
        -1,
        "invalid"
    ]
)
def test_post_process_data_rejects_invalid_outlier_threshold(compute_mean, threshold):
        """
        tests that the class raises an exception if the thresholdDeviationFromMedian
        is not a valid positive number
        """

        preprocessed_data = {
            "station-one": build_series_obj(
                ["10", "12", "14"],
                sentinel_value=1000,
            ),
            "station-two": build_series_obj(
                ["12", "14", "50"],
                sentinel_value=1000,
            )
        }

        post_process_call = PostProcessCall()
        post_process_call.call = "ComputeMean"
        post_process_call.args = {
            "target_inKeys": [
                "station-one",
                "station-two"
            ],
            "dropOutlierValues": True,
            "thresholdDeviationFromMedian": threshold,
            "outKey": "ESB-combined-water-temp"
        }

        with pytest.raises((TypeError, ValueError)):
            compute_mean.post_process_data(preprocessed_data, post_process_call)


def test_post_process_data_handles_different_series_sentinels(compute_mean):
        """
        tests that different sentinel values in different input series are all replaced with NaN
        when computing the mean
        """

        preprocessed_data = {
            "station-one": build_series_obj(
                ["10", "1000", "14"],
                sentinel_value=1000,
            ),
            "station-two": build_series_obj(
                ["12", "-999", "16"],
                sentinel_value=-999,
            ),
            "station-three": build_series_obj(
                ["14", "18", "missing"],
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
            "dropOutlierValues": False,
            "outKey": "ESB-combined-water-temp",
        }

        result = compute_mean.post_process_data(
            preprocessed_data,
            post_process_call,
        )

        output_df = result["ESB-combined-water-temp"].dataFrame

        # assert the overall means were calculated correctly, ignoring the sentinel values
        assert output_df["dataValue"].astype(float).tolist() == (pytest.approx([12.0, 18.0, 15.0]))


def test_post_process_data_sets_unused_metadata_to_null(compute_mean):
    """
    tests that the output series retains the required metadata from the template series
    and sets the other unused metadata to null
    """

    preprocessed_data = {
        "station-one": build_series_obj(
            ["10", "12", "14"],
            sentinel_value=1000,
        ),
        "station-two": build_series_obj(
            ["12", "14", "16"],
            sentinel_value=1000,
        ),
    }

    post_process_call = PostProcessCall()
    post_process_call.call = "ComputeMean"
    post_process_call.args = {
        "target_inKeys": [
            "station-one",
            "station-two",
        ],
        "dropOutlierValues": False,
        "outKey": "ESB-combined-water-temp",
    }

    result = compute_mean.post_process_data(preprocessed_data, post_process_call)

    output_series = result["ESB-combined-water-temp"]
    output_df = output_series.dataFrame

    assert (output_series.description.dataSeries == "ESB-combined-water-temp")

    # dataLocation is required and comes from the template series.
    assert output_series.description.dataLocation == "Test"

    # The computed series does not have its own datum.
    assert output_series.description.dataDatum is None

    for column in [
        "dataUnit",
        "timeGenerated",
        "latitude",
        "longitude",
    ]:
        assert output_df[column].isna().all()

    assert output_series.sentinelValue is None


def test_post_process_data_does_not_modify_input_series(compute_mean):
        """
        Post-processing should not mutate any input series
        """

        preprocessed_data = {
            "station-one": build_series_obj(
                ["10", "1000", "14"],
                sentinel_value=1000,
            ),
            "station-two": build_series_obj(
                ["12", "14", "16"],
                sentinel_value=1000,
            ),
        }

        original_dataframes = {
            key: series.dataFrame.copy(deep=True)
            for key, series in preprocessed_data.items()
        }

        post_process_call = PostProcessCall()
        post_process_call.call = "ComputeMean"
        post_process_call.args = {
            "target_inKeys": [
                "station-one",
                "station-two"
            ],
            "dropOutlierValues": False,
            "outKey": "ESB-combined-water-temp"
        }

        compute_mean.post_process_data(preprocessed_data, post_process_call)

        for key, expected_df in original_dataframes.items():
            pd.testing.assert_frame_equal(
                preprocessed_data[key].dataFrame,
                expected_df
            )


@pytest.mark.parametrize(
    "preprocessed_data",
    [
        {
            # empty lists
            "station-one": build_series_obj(
                [],
                sentinel_value=1000
            ),
            "station-two": build_series_obj(
                [],
                sentinel_value=1000
            )
        },
        {
            # lists of None
            "station-one": build_series_obj(
                [None, None, None],
                sentinel_value=1000
            ),
            "station-two": build_series_obj(
                [None, None, None],
                sentinel_value=1000
            ) 
        },
        {
            # lists of np.nan
            "station-one": build_series_obj(
                [np.nan, np.nan, np.nan],
                sentinel_value=1000
            ),
            "station-two": build_series_obj(
                [np.nan, np.nan, np.nan],
                sentinel_value=1000
            ) 
        },
        {
            # lists of "nan" strings
            "station-one": build_series_obj(
                ['nan', 'nan', 'nan'],
                sentinel_value=1000
            ),
            "station-two": build_series_obj(
                ['nan', 'nan', 'nan'],
                sentinel_value=1000
            ) 
        },
        {
            # mix of None, np.nan, and "nan" strings
            "station-one": build_series_obj(
                [None, np.nan, 'nan'],
                sentinel_value=1000
            ),
            "station-two": build_series_obj(
                [np.nan, 'None', 'nan'],
                sentinel_value=1000
            )
        },
        {
            # all values are dropped by sentinel value
            "station-one": build_series_obj(
                [1000, 1000, 1000],
                sentinel_value=1000
            ),
            "station-two": build_series_obj(
                [1000, 1000, 1000],
                sentinel_value=1000
            )
        }
    ],
    ids=[
        "empty-lists",
        "lists-of-None",
        "lists-of-nan",
        "lists-of-string-nan",
        "mixed-None-np.nan-string-nan",
        "all-dropped-values"
    ]
)
def test_post_process_data_handles_empty_series(compute_mean, preprocessed_data):
    """
    tests that ComputeMean can handle various cases of empty/invalid inputs.
    """
    #preprocessed_data["station-one"].dataFrame['dataValue']

    post_process_call = PostProcessCall()
    post_process_call.call = "ComputeMean"
    post_process_call.args = {
        "target_inKeys": [
            "station-one",
            "station-two"
        ],
        "dropOutlierValues": False,
        "outKey": "ESB-combined-water-temp"
    }

    with pytest.raises(ValueError):
        result = compute_mean.post_process_data(preprocessed_data, post_process_call)
