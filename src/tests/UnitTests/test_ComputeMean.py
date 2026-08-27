# -*- coding: utf-8 -*-
# test_ComputeMean.py
# -------------------------------
# Created By: Anointiyae Beasley
# Created Date: 08/19/2026
# -------------------------------

"""Tests for the ComputeMean post-processing class.

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

    assert output_df["dataUnit"].isna().all()

    assert (
        output_series.description.dataSeries
        == "combined-water-temp"
    )

    # The computed series should not inherit an input station's sentinel.
    assert output_series.sentinelValue is None


def test_get_station_values_rejects_non_numeric_non_sentinel(
    compute_mean,
):
    """A nonnumeric value that is not the sentinel should raise an error."""
    station_series = build_series_obj(
        ["10", "invalid", "14"],
        sentinel_value=1000,
    )

    with pytest.raises(ValueError):
        compute_mean.get_station_values(
            "station-one",
            station_series,
        )


def test_get_station_values_replaces_string_sentinel(
    compute_mean,
):
    """An exact string sentinel should be replaced with NaN."""
    station_series = build_series_obj(
        ["10", "missing", "14"],
        sentinel_value="missing",
    )

    result = compute_mean.get_station_values(
        "station-one",
        station_series,
    )

    assert result.iloc[0] == pytest.approx(10.0)
    assert pd.isna(result.iloc[1])
    assert result.iloc[2] == pytest.approx(14.0)


def test_post_process_data_raises_when_all_values_missing(
    compute_mean,
):
    """Fail when no station has a valid value for a timestamp."""
    preprocessed_data = {
        "station-one": build_series_obj(
            ["10", "1000", "14"],
            sentinel_value=1000,
        ),
        "station-two": build_series_obj(
            ["12", "1000", "16"],
            sentinel_value=1000,
        ),
        "station-three": build_series_obj(
            ["14", "missing", "18"],
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

    with pytest.raises(
        ValueError,
        match="no valid station values remained",
    ):
        result = compute_mean.post_process_data(
            preprocessed_data,
            post_process_call,
        )


def test_post_process_data_uses_union_of_timestamps(
        compute_mean,
    ):
        """Timestamps present in only one station should remain in the output."""
        station_one = build_series_obj(
            ["10", "12", "14"],
            sentinel_value=1000,
        )

        station_two = build_series_obj(
            ["20", "22", "24"],
            sentinel_value=1000,
        )

        # Remove station two's first timestamp.
        station_two.dataFrame = (
            station_two.dataFrame.iloc[1:]
            .reset_index(drop=True)
        )

        preprocessed_data = {
            "station-one": station_one,
            "station-two": station_two,
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

        result = compute_mean.post_process_data(
            preprocessed_data,
            post_process_call,
        )

        output_df = result[
            "ESB-combined-water-temp"
        ].dataFrame

        assert output_df["timeVerified"].tolist() == (
            EXPECTED_TIMESTAMPS
        )

        assert output_df["dataValue"].astype(float).tolist() == (
            pytest.approx([10.0, 17.0, 19.0])
        )


def test_post_process_data_raises_for_missing_input_key(
        compute_mean,
    ):
        """A requested key missing from the repository should raise an error."""
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

        with pytest.raises(Exception):
            compute_mean.post_process_data(
                preprocessed_data,
                post_process_call,
            )


def test_post_process_data_raises_for_empty_target_keys(
        compute_mean,
    ):
        """ComputeMean should reject an empty input-key list."""
        post_process_call = PostProcessCall()
        post_process_call.call = "ComputeMean"
        post_process_call.args = {
            "target_inKeys": [],
            "dropOutlierValues": False,
            "outKey": "ESB-combined-water-temp",
        }

        with pytest.raises(Exception):
            compute_mean.post_process_data(
                {},
                post_process_call,
            )


@pytest.mark.parametrize(
        "threshold",
        [
            None,
            -1,
            "invalid",
        ],
    )
def test_post_process_data_rejects_invalid_outlier_threshold(
        compute_mean,
        threshold,
    ):
        """Outlier removal requires a valid, non-negative threshold."""
        preprocessed_data = {
            "station-one": build_series_obj(
                ["10", "12", "14"],
                sentinel_value=1000,
            ),
            "station-two": build_series_obj(
                ["12", "14", "50"],
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
            "dropOutlierValues": True,
            "thresholdDeviationFromMedian": threshold,
            "outKey": "ESB-combined-water-temp",
        }

        with pytest.raises(
            (TypeError, ValueError)
        ):
            compute_mean.post_process_data(
                preprocessed_data,
                post_process_call,
            )


def test_post_process_data_handles_different_station_sentinels(
        compute_mean,
    ):
        """Each input station should use its own configured sentinel."""
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

        output_df = result[
            "ESB-combined-water-temp"
        ].dataFrame

        assert output_df["dataValue"].astype(float).tolist() == (
            pytest.approx([12.0, 18.0, 15.0])
        )


def test_post_process_data_sets_unused_metadata_to_null(
    compute_mean,
):
    """Retain required location metadata and null unused metadata."""
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

    result = compute_mean.post_process_data(
        preprocessed_data,
        post_process_call,
    )

    output_series = result[
        "ESB-combined-water-temp"
    ]
    output_df = output_series.dataFrame

    assert (
        output_series.description.dataSeries
        == "ESB-combined-water-temp"
    )

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


def test_post_process_data_does_not_modify_input_series(
        compute_mean,
    ):
        """Post-processing should not mutate any source DataFrame."""
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
                "station-two",
            ],
            "dropOutlierValues": False,
            "outKey": "ESB-combined-water-temp",
        }

        compute_mean.post_process_data(
            preprocessed_data,
            post_process_call,
        )

        for key, expected_df in original_dataframes.items():
            pd.testing.assert_frame_equal(
                preprocessed_data[key].dataFrame,
                expected_df,
            )
