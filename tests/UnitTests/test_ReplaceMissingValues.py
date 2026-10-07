# -*- coding: utf-8 -*-
# test_ReplaceMissingValues.py
# ----------------------------------
# Created By: Anointiyae Beasley
# Created Date: 08/11/2026
# ----------------------------------
"""Tests for the ReplaceMissingValues data-integrity operation.

Run:
    docker exec semaphore-core python3 -m pytest \
        src/tests/UnitTests/test_ReplaceMissingValues.py
"""

import sys
from datetime import datetime, timedelta, timezone

sys.path.append("/app/src")

import pytest
from pandas import DataFrame

from src.DataClasses import (
    DataIntegrityDescription,
    Series,
    SeriesDescription,
    TimeDescription,
    get_input_dataFrame,
)
from src.DataIntegrity.IDataIntegrity import data_integrity_factory


UTC = timezone.utc
START = datetime(2024, 1, 1, hour=0, tzinfo=UTC)
END = datetime(2024, 1, 1, hour=10, tzinfo=UTC)

TEST_TIME_DESCRIPTION = TimeDescription(
    START,
    END,
    timedelta(hours=1),
)


def make_input_df(values_by_hour: dict[int, object]) -> DataFrame:
    """Build an input DataFrame containing the supplied verification hours."""
    data_frame = get_input_dataFrame()

    for row_number, (hour, data_value) in enumerate(values_by_hour.items()):
        data_frame.loc[row_number] = [
            data_value,
            "test",
            START + timedelta(hours=hour),
            START,
            None,
            None,
        ]

    return data_frame


def make_series_description(sentinel_value: object) -> SeriesDescription:
    """Build a SeriesDescription configured for ReplaceMissingValues."""
    return SeriesDescription(
        "NOAATANDC",
        "dWnDir",
        "packChan",
        dataIntegrityDescription=DataIntegrityDescription(
            "ReplaceMissingValues",
            {"sentinel_value": sentinel_value},
        ),
    )


@pytest.mark.parametrize(
    "sentinel_value, values_by_hour, expected_values",
    [
        pytest.param(
            10000,
            {0: "0.6", 1: "0.66", 2: "0.69", 6: "0.72", 7: "0.76"},
            [
                "0.6", "0.66", "0.69", 10000, 10000, 10000,
                "0.72", "0.76", 10000, 10000, 10000,
            ],
            id="numeric-sentinel-fills-missing-timestamps",
        ),
        pytest.param(
            "missing",
            {0: "0.6", 1: "0.66", 2: "0.69", 6: "0.72", 7: "0.76"},
            [
                "0.6", "0.66", "0.69", "missing", "missing", "missing",
                "0.72", "0.76", "missing", "missing", "missing",
            ],
            id="string-sentinel-fills-missing-timestamps",
        ),
        pytest.param(
            "missing",
            {0: "None", 1: "0.66", 2: "0.69", 6: "0.72", 7: None},
            [
                "missing", "0.66", "0.69", "missing", "missing", "missing",
                "0.72", "missing", "missing", "missing", "missing",
            ],
            id="actual-null-and-string-none-are-replaced",
        ),
        pytest.param(
            "missing",
            {0: "calm", 1: "variable", 2: "N", 6: "SW", 7: "0.76"},
            [
                "calm", "variable", "N", "missing", "missing", "missing",
                "SW", "0.76", "missing", "missing", "missing",
            ],
            id="valid-nonnumeric-strings-are-preserved",
        ),
    ],
)
def test_replace_missing_values(
    sentinel_value: object,
    values_by_hour: dict[int, object],
    expected_values: list[object],
):
    series_description = make_series_description(sentinel_value)
    in_series = Series(
        description=series_description,
        timeDescription=TEST_TIME_DESCRIPTION,
    )
    in_series.dataFrame = make_input_df(values_by_hour)

    data_integrity_class = data_integrity_factory(
        series_description.dataIntegrityDescription.call
    )
    out_series = data_integrity_class.exec(in_series)

    expected_timestamps = [
        START + timedelta(hours=hour)
        for hour in range(11)
    ]
    # print(f'inSeries.dataFrame: {in_series.dataFrame}')
    # print(f'outSeries.dataFrame: {out_series.dataFrame}')
    
    # print(f'outSeries.dataFrame dataValue List: {out_series.dataFrame["dataValue"].values.tolist()}')
    # print(f'expected_df dataValue List: {expected_values}')

    assert out_series.dataFrame["timeVerified"].tolist() == expected_timestamps
    assert out_series.dataFrame["dataValue"].tolist() == expected_values
    assert out_series.sentinelValue == sentinel_value