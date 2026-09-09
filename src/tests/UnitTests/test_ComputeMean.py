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
from src.PostProcessing.PostProcessingClasses.ComputeMean import Semaphore_Data_Exception

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


def build_timestamps(length: int, interval: timedelta = timedelta(hours=1), start: datetime = START) -> list[datetime]:
    """Generate [length] timestamps spaced [interval] apart, starting at [start]"""
    return [start + (interval * i) for i in range(length)]


def build_time_description(length: int, interval: timedelta = timedelta(hours=1), start: datetime = START) -> TimeDescription:
    """Build a TimeDescription whose fromDateTime/toDateTime match exactly [length] timestamps"""
    timestamps = build_timestamps(length, interval, start)
    return TimeDescription(fromDateTime=timestamps[0], toDateTime=timestamps[-1], interval=interval)


def build_series_obj(
    data: list[object],
    sentinel_value: int | str | None,
    interval: timedelta = timedelta(hours=1),
    start: datetime = START,
) -> Series:
    """Build one Series whose timestamps/time description match len(data)"""
    timestamps = build_timestamps(len(data), interval, start)
    time_description = build_time_description(len(data), interval, start)

    series = Series(SeriesDescription("Test", "water-temp", "Test"), time_description)

    data_frame = get_input_dataFrame()
    for index, value in enumerate(data):
        data_frame.loc[index] = [
            str(value),
            "degrees_C",
            timestamps[index],
            start,
            None,
            None,
        ]

    series.dataFrame = data_frame
    series.sentinelValue = sentinel_value

    return series


@pytest.mark.parametrize(
    (
        "drop_outliers",
        "threshold",
        "input_lists",
        "sentinel_values",
        "expected_values"
    ),
    [   # test with no sentinel values and no outlier removal
        (
            False,
            None,
            [
                [1, 2, 3],  # series 1's values
                [4, 5, 6],  # series 2's values
                [7, 8, 9]   # series 3's values
            ],
            [1000, 2000, 3000],  # 1000 is sentinel for series 1, 2000 for series 2, 3000 for series 3
            [4.0, 5.0, 6.0]  # expected mean values
        ),
        # test with a sentinel value, but no outlier removal
        (
            False,
            None,
            [
                [1, 2, 3],      # series 1's values
                [4, 5, 6],      # series 2's values
                [7, 8, 3000]     # series 3's values, 3000 is a sentinel
            ],
            [1000, 2000, 3000],
            # expected mean values, the sentinel should not affect the mean calculation
            # so the mean for row 3 should be (3 + 6) / 2 = 4.5
            [4.0, 5.0, 4.5]  
        ),
        # test with more sentinel values, but no outlier removal
        (
            False,
            None,
            [
                [1, 'missing', 3],
                [2000, 5, 6],
                [7, 8, 'sentinel']
            ],
            ['missing', 2000, 'sentinel'],
            # sentinels shouldn't affect the mean calculations
            [4.0, 6.5, 4.5]
        ),
        # test with outlier removal, but no sentinel values
        (
            True,
            3.5,
            [
                [1, 50, 3],
                [40, 5, 6],
                [3, 4, 100]
            ],
            [1000, 2000, 3000], # no values are removed due to sentinels
            # expected mean values, the outliers should be removed before calculating the mean
            [2.0, 4.5, 4.5]
        ),
        # test with both sentinel values and outlier removal
        # also tests for longer series beyond 3 timestamps
        (
            True,
            25,
            [
                # sentinel = 999 and outliers = 1000 and -1000
                [10, 999, 10, 10, 10, 1000, 10, 999, -1000, 40],

                # sentinel = 'missing' and outliers = 1000
                [20, 20, 'missing', 20, 20, 20, 1000, 'missing', 30, 50],

                # sentinel = -1 and outliers = 1000
                [30, 30, 30, -1, 1000, 30, 30, 30, 1000, -1],
            ],
            [999, 'missing', -1],
            [
                20.0,  # timestamp 0: 20 from (10 + 20 + 30) / 3
                25.0,  # timestamp 1: (20 + 30) / 2; 999 sentinel is ignored
                20.0,  # timestamp 2: (10 + 30) / 2; 'missing' sentinel is ignored
                15.0,  # timestamp 3: (10 + 20) / 2; -1 sentinel is ignored
                15.0,  # timestamp 4: (10 + 20) / 2; 1000 outlier is ignored
                25.0,  # timestamp 5: (20 + 30) / 2; 1000 outlier is ignored
                20.0,  # timestamp 6: (10 + 30) / 2; 1000 outlier is ignored
                30.0,  # timestamp 7: 30; 999 and 'missing' sentinels are ignored
                30.0,  # timestamp 8: 30; -1000 and 1000 outliers are ignored
                45.0,  # timestamp 9: (40 + 50) / 2; -1 sentinel is ignored
            ]
        ),
        # tests that when only 1 series has values for a timestamp, the mean is just that value
        (
            False,
            None,
            [
                [1,    1000, 1000, 4,    1000, 1000, 7,    1000, 1000, 10,   1000, 1000],
                [1000, 2,    1000, 1000, 5,    1000, 1000, 8,    1000, 1000, 11,   1000],
                [1000, 1000, 3,    1000, 1000, 6,    1000, 1000, 9,    1000, 1000, 12]
            ],
            [1000, 1000, 1000],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        ),
        # tests that values exactly equal to the threshold are dropped
        (
            True,
            5,
            [
                [10, 20, 30, 40, 50],
                [15, 25, 35, 45, 55],
                [20, 30, 40, 50, 60]
            ],
            [1000, 1000, 1000],
            [
                15.0, # 10 and 20 are both exactly 5 away from the median of 15, so both are dropped, leaving only 15
                25.0,
                35.0,
                45.0,
                55.0
            ]
        )
    ],
    ids=[
        "basic-test",
        "sentinel-no-outliers",
        "multiple-sentinels-no-outliers",
        "no-sentinels-with-outliers",
        "sentinels-with-outliers-long-series",
        "single-value-per-timestamp",
        "values-equal-to-threshold"
    ]
)
def test_post_process_data_combines_multiple_series(
    compute_mean,
    drop_outliers,
    threshold, 
    input_lists,
    sentinel_values,
    expected_values
):
    """
    Tests that the overall post_process_data function works as intended
    """
    preprocessed_data = {
        "station-one": build_series_obj(
            input_lists[0],
            sentinel_value=sentinel_values[0],
        ),
        "station-two": build_series_obj(
            input_lists[1],
            sentinel_value=sentinel_values[1],
        ),
        "station-three": build_series_obj(
            input_lists[2],
            sentinel_value=sentinel_values[2],
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
        "thresholdDeviationFromMedian": threshold,
        "outKey": "combined-water-temp",
    }
 
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

    # check for overall correctness of the ComputeMean class

    # ensure correct values were computed
    assert actual_values == pytest.approx(expected_values)

    # ensure outkey was used in description          
    assert output_series.description.dataSeries == "combined-water-temp"

    # the output series should have been added to the data repository
    assert output_series.description.dataSeries in preprocessed_data

    # the output series should have the same number of timestamps as the input series
    # and all input series have the same timestamps
    assert output_df["timeVerified"].tolist() == preprocessed_data["station-one"].dataFrame["timeVerified"].tolist()

    # check dataframe metadata is null since the computed series' metadata
    # may not represent the metadata of all the input series
    assert output_df["dataUnit"].isna().all()
    assert output_df["timeGenerated"].isna().all()
    assert output_df["latitude"].isna().all()
    assert output_df["longitude"].isna().all()

    # check series object metadata
    assert output_series.sentinelValue is None
    assert output_series.description.dataSource is None
    assert output_series.description.dataLocation is None
    assert output_series.description.dataDatum is None
    assert output_series.timeDescription.stalenessOffset is None


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


def test_post_process_data_raises_for_missing_timestamp(compute_mean):
        """
        Tests that ComputeMean raises a ValueError when an input series that is missing
        a timestamp. This is checked by comparing the number of timestamps in each series,
        not the actual timestamp values. 
        
        See test_post_process_data_raises_for_mismatched_time_description() for testing mismatched timestamp values.
        """
        station_one = build_series_obj(
            ["10", "12", "14"],
            sentinel_value=1000
        )

        station_two = build_series_obj(
            ["20", "22", "24"],
            sentinel_value=1000
        )

        # remove station two's first timestamp
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

        with pytest.raises(ValueError, match="has a different number of timestamps"):
            compute_mean.post_process_data(preprocessed_data, post_process_call)


@pytest.mark.parametrize(
    "mismatched_time_description",
    [
        # test with a series that has a different fromDateTime
        TimeDescription(
            START + timedelta(hours=1),
            START + timedelta(hours=3),
            timedelta(hours=1),
        ),
        # test with a series that has a different toDateTime
        TimeDescription(
            START,
            START + timedelta(hours=3),
            timedelta(hours=1),
        ),
        # test with a series that has a different interval
        TimeDescription(
            START,
            START + timedelta(hours=2),
            timedelta(hours=2),
        )
    ],
    ids=[
        "different-fromDateTime",
        "different-toDateTime",
        "different-interval",
    ]
)
def test_post_process_data_raises_for_mismatched_time_description(compute_mean, mismatched_time_description):
        """
        Tests that ComputeMean raises a ValueError when an input series' time description
        (fromDateTime, toDateTime, or interval) does not match the other input series' time
        description, even when both series have the same number of timestamps.
        """
        station_one = build_series_obj(
            ["10", "12", "14"],
            sentinel_value=1000
        )

        station_two = build_series_obj(
            ["20", "22", "24"],
            sentinel_value=1000
        )

        # set station two's time description to a mismatched one
        station_two.timeDescription = mismatched_time_description

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

        with pytest.raises(ValueError, match="has a different time description"):
            compute_mean.post_process_data(preprocessed_data, post_process_call)


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

    # the output dataSeries should match the outKey specified in the post_process_call
    assert (output_series.description.dataSeries == "ESB-combined-water-temp")

    # check other metadata was set to None
    assert output_series.description.dataSource is None
    assert output_series.description.dataLocation is None
    assert output_series.description.dataDatum is None
    assert output_series.sentinelValue is None      # output shouldn't have a sentinel value since it is a computed series

    # metadata in the dataframe should be null
    for column in ["dataUnit", "timeGenerated", "latitude", "longitude"]:
        assert output_df[column].isna().all()


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


def test_post_process_data_ignores_unrelated_series_in_preprocessed_data(compute_mean):
    """
    Tests that ComputeMean ignores any series in the preprocessed_data repository that
    is not used as a target_inKey. The unrelated series should remain in the repository after the post-processing is complete
    and should not be modified in any way.
    """
    preprocessed_data = {
        "station-one": build_series_obj(["10", "12", "14"], sentinel_value=1000),
        "station-two": build_series_obj(["20", "22", "24"], sentinel_value=1000),
        "unrelated-series": build_series_obj(["1", "2", "3", "4", "5"], sentinel_value=1000),
    }

    post_process_call = PostProcessCall()
    post_process_call.call = "ComputeMean"
    post_process_call.args = {
        "target_inKeys": ["station-one", "station-two"],
        "dropOutlierValues": False,
        "outKey": "combined-water-temp",
    }

    # check the regular result got added
    result = compute_mean.post_process_data(preprocessed_data, post_process_call)
    assert "combined-water-temp" in result

    # check the unrelated series is still present in the result and was not modified
    assert "unrelated-series" in result
    assert result["unrelated-series"].dataFrame.equals(preprocessed_data["unrelated-series"].dataFrame)

@pytest.mark.parametrize(
    "series_values",
    [
        (
            [None, None, None]
        ),
        (
            [np.nan, np.nan, np.nan]
        ),
        (
            ['nan', 'nan', 'nan']
        ),
        (
            [None, np.nan, 'nan']
        )
    ],
    ids=[
        "list-of-None",
        "list-of-nan",
        "list-of-string-nan",
        "mixed",
    ]
)
def test_get_series_values_raises_value_error(compute_mean, series_values):
    """
    tests that _get_series_values() raises a ValueError when the dataValue column cannot be converted to numeric
    """

    series = build_series_obj(series_values, sentinel_value=1000)

    # all of these cases should fail since the dataValue column cannot be converted to numeric
    with pytest.raises(ValueError, match="dataValue column to numeric. Ensure the series contains only numeric values or the series' sentinel value."):
        result = compute_mean._get_series_values(series)


@pytest.mark.parametrize(
    "row, drop_outliers, threshold",
    [   
        # this test means that 3 series all have None for a specific timestamp, so the mean cannot be computed
        (
            pd.Series([None, None, None]), False, None
        ),
        # should raise since nans are dropped and the row becomes empty
        (
            pd.Series([np.nan, np.nan, np.nan]), False, None
        ),
        # empty list
        (
            pd.Series([]), False, None
        )
    ]
)
def test_compute_mean_function_raises_semaphore_data_exception(compute_mean, row, drop_outliers, threshold):
    """
    tests that _compute_mean() raises a Semaphore Data Exception when the row is
    empty after dropping nans. 
    
    NOTE: It is impossible to have an empty row by dropping all outliers since
    the median deviates 0 from the median, and 0 will always be less than any
    positive threshold set.
    """
    with pytest.raises(Semaphore_Data_Exception):
        result = compute_mean._compute_mean(row, drop_outliers, threshold)