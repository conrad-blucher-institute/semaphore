# -*- coding: utf-8 -*-
#test_LIGHTHOUSE.py
#-------------------------------
# Created By: Matthew Kastl
# Created Date: 11/2/2023
# Version: 1.0
#----------------------------------
"""This file tests the LIGHTHOUSE ingestion class and its functions. 

run: docker exec semaphore-core python3 -m pytest src/tests/IntegrationTests/test_LIGHTHOUSE.py
 """ 
#----------------------------------
# 
#
#Imports
import sys
sys.path.append('/app/src')

import pytest

from datetime import datetime, timedelta, time, date, timezone
from src.DataClasses import TimeDescription, SeriesDescription, Series
from src.DataIngestion.IDataIngestion import data_ingestion_factory
from src.DataIngestion.DI_Classes.LIGHTHOUSE import LIGHTHOUSE
from dotenv import load_dotenv
from unittest.mock import patch

@pytest.mark.slow

@pytest.mark.parametrize("seriesDescription, timeDescription, expected_min_output", [
    
    # lowering number of expected points for hourly interval from 241 to 220 to account for some missing data points in lighthouse during this time range
    # series: dWaterTmp - wtp
    (SeriesDescription('LIGHTHOUSE', 'dWaterTmp', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0), tzinfo=timezone.utc), datetime.combine(date(2023, 9, 7), time(11, 0), tzinfo=timezone.utc), timedelta(seconds=3600)), 220), # 1hr interval
    (SeriesDescription('LIGHTHOUSE', 'dWaterTmp', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0), tzinfo=timezone.utc), datetime.combine(date(2023, 9, 6), time(12, 0), tzinfo=timezone.utc), timedelta(seconds=360)), 11), # 6min interval
    (SeriesDescription('LIGHTHOUSE', 'dWaterTmp', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0), tzinfo=timezone.utc), datetime.combine(date(2023, 9, 6), time(12, 0), tzinfo=timezone.utc), None), 11), # no interval
    # series: dAirTmp - atp
    (SeriesDescription('LIGHTHOUSE', 'dAirTmp', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0), tzinfo=timezone.utc), datetime.combine(date(2023, 9, 7), time(11, 0), tzinfo=timezone.utc), timedelta(seconds=3600)), 220), # 1hr interval
    (SeriesDescription('LIGHTHOUSE', 'dAirTmp', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0), tzinfo=timezone.utc), datetime.combine(date(2023, 9, 6), time(12, 0), tzinfo=timezone.utc), timedelta(seconds=360)), 11),  # 6min interval
    (SeriesDescription('LIGHTHOUSE', 'dAirTmp', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0), tzinfo=timezone.utc), datetime.combine(date(2023, 9, 6), time(12, 0), tzinfo=timezone.utc), None), 11), # no interval
    # series: pHarm 
    (SeriesDescription('LIGHTHOUSE', 'pHarm', 'PortLavaca', 'MLLW'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0), tzinfo=timezone.utc), datetime.combine(date(2023, 9, 6), time(12, 0), tzinfo=timezone.utc), timedelta(seconds=360)), 11),
    # series: erroneous
    (SeriesDescription('LIGHTHOUSE', 'apple', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0), tzinfo=timezone.utc), datetime.combine(date(2023, 9, 7), time(11, 0), tzinfo=timezone.utc), timedelta(seconds=3600)), None),
])
def test_pull_pd_endpoint_dataPoint(seriesDescription: SeriesDescription, timeDescription: TimeDescription, expected_min_output: int | None):
    """This function tests the pull_pd_endpoint_dataPoint which is datapoints from LIGHTHOUSE's pd endpoint
        function is complete.
        After refactor: Tests that all valid data points within time range are returned (no interval filtering).
        NOTE:: This test depends on DBM.insert_external_location_code('SouthBirdIsland', 'LIGHTHOUSE', '013', 0) pre=existing in db
    """
    load_dotenv()

    lighthouse = LIGHTHOUSE()
    result: Series = lighthouse._LIGHTHOUSE__pull_pd_endpoint_dataPoint(seriesDescription, timeDescription)

    if expected_min_output is None:
        assert result is None
    else:
        assert result is not None
        assert len(result.dataFrame) >= expected_min_output
        
        # Verify that all returned data points are within the requested time range
        if len(result.dataFrame) > 0:
            start_time = timeDescription.fromDateTime.timestamp()
            end_time = timeDescription.toDateTime.timestamp()
            
            for _, row in result.dataFrame.iterrows():
                row_timestamp = row['timeVerified'].timestamp()
                assert start_time <= row_timestamp <= end_time, f"Data point {row['timeVerified']} is outside requested range"


@pytest.mark.slow

@pytest.mark.parametrize("seriesDescription, timeDescription, expected_output", [
    # series: erroneous
    (SeriesDescription('LIGHTHOUSE', 'apple', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0), tzinfo=timezone.utc), datetime.combine(date(2023, 9, 6), time(11, 0), tzinfo=timezone.utc), timedelta(seconds=3600)), None),
])
def test_ingest_series(seriesDescription: SeriesDescription, timeDescription: TimeDescription, expected_output: None):
    """This function tests the test ingest series method. This test makes sure that the LIGHTHOUSE ingestion class is being properly mapped
        function is complete.
    """
    load_dotenv()

    lighthouse = data_ingestion_factory(seriesDescription)
    result = lighthouse.ingest_series(seriesDescription, timeDescription)

    if expected_output is None:
        assert result is None
    elif expected_output == "has_data":
        assert result is not None
        assert len(result.dataFrame) > 0

@pytest.mark.slow
def test_prediction_timeGenerated_is_current():
    """This function tests that prediction series (prefix 'p') sets timeGenerated to current time rather than the datapoint's verified time.
    
    For prediction series, timeGenerated represents when the data was ingested. All rows from a single ingestion call should share the same
    timeGenerated timestamp, and that timestamp should differ from the historical timeVerified values.
    """
    load_dotenv()

    lighthouse = LIGHTHOUSE()

    # Use historical data — the 2023 timeVerified values will be clearly distinct
    # from any timeGenerated stamp set at ingestion time (2025+)
    seriesDescription = SeriesDescription('LIGHTHOUSE', 'pHarm', 'PortLavaca', 'MLLW')
    timeDescription = TimeDescription(
        datetime.combine(date(2023, 9, 6), time(11, 0), tzinfo=timezone.utc),
        datetime.combine(date(2023, 9, 6), time(12, 0), tzinfo=timezone.utc),
        timedelta(seconds=360)
    )

    result: Series = lighthouse._LIGHTHOUSE__pull_pd_endpoint_dataPoint(seriesDescription, timeDescription)

    assert result is not None
    assert len(result.dataFrame) > 0

    # All rows from a single ingestion call should share the same timeGenerated,
    # since they were all stamped at the moment of the call — not at their individual verified/generated times.
    unique_time_generateds = result.dataFrame['timeGenerated'].unique()
    assert len(unique_time_generateds) == 1, (
        f"Expected all rows to share one timeGenerated (set at ingestion), "
        f"but found {len(unique_time_generateds)} distinct values: {unique_time_generateds}"
    )

    # The single timeGenerated should not match the first row's timeVerified.
    # If they were equal, it would indicate timeGenerated was mistakenly set to timeVerified
    # rather than stamped at ingestion time. 
    ingestion_time = result.dataFrame['timeGenerated'].iloc[0]
    first_verified_time = result.dataFrame['timeVerified'].iloc[0]
    assert ingestion_time != first_verified_time, (
        f"timeGenerated ({ingestion_time}) should not equal timeVerified ({first_verified_time}) "
        f"for prediction series — timeGenerated should reflect ingestion time, not the historical verified time"
    )

def test_now_point_estimated_from_sub_hourly_data():
    """Tests that when the now-point (toDateTime) is missing from Lighthouse's hourly data,
    the ingestion class synthesizes it by averaging any sub-hourly readings published
    in the extended fetch window past toDateTime.

    The API response is mocked to return:
      - Clean hourly data up to but NOT including toDateTime (simulating a delayed hourly publish)
      - Two 6-minute readings just past toDateTime (simulating sub-hourly data that arrived first)

    Expected behavior:
      - The returned series contains a row at toDateTime
      - That row's dataValue is the average of the two sub-hourly readings
      - timeGenerated for that row is close to now (not a historical timestamp)
    """
    load_dotenv()

    lighthouse = LIGHTHOUSE()

    # Use a fixed historical window 
    to_dt = datetime.combine(date(2023, 9, 6), time(12, 0), tzinfo=timezone.utc)
    from_dt = datetime.combine(date(2023, 9, 6), time(11, 0), tzinfo=timezone.utc)

    seriesDescription = SeriesDescription('LIGHTHOUSE', 'dWaterTmp', 'SouthBirdIsland')
    timeDescription = TimeDescription(from_dt, to_dt, timedelta(seconds=3600))

    # Build a fake API response:
    # - One hourly reading at fromDateTime (11:00)
    # - NO reading at toDateTime (12:00) — simulates delayed hourly publish
    # - Two 6-minute readings at 12:06 and 12:12 — simulates sub-hourly data that arrived first
    sub_hourly_val_1 = 22.0
    sub_hourly_val_2 = 22.6
    expected_average = (sub_hourly_val_1 + sub_hourly_val_2) / 2

    mock_data = {
        '013': {
            'lat': 27.4847,
            'lon': -97.3181,
            'data': {
                'wtp': [
                    [int(from_dt.timestamp() * 1000), 21.5],                               # 11:00 — clean hourly point
                    [int((to_dt.timestamp() + 360) * 1000), sub_hourly_val_1],              # 12:06 — sub-hourly
                    [int((to_dt.timestamp() + 720) * 1000), sub_hourly_val_2],              # 12:12 — sub-hourly
                ]
            }
        }
    }

    with patch.object(lighthouse, '_LIGHTHOUSE__api_request', return_value=mock_data):
        result = lighthouse._LIGHTHOUSE__pull_pd_endpoint_dataPoint(seriesDescription, timeDescription)

    assert result is not None, "Expected a series to be returned"

    # Confirm the now-point slot is present
    result_df = result.dataFrame
    now_point_rows = result_df[result_df['timeVerified'] == to_dt]
    assert len(now_point_rows) == 1, (
        f"Expected exactly one row at toDateTime {to_dt}, found {len(now_point_rows)}"
    )

    # Confirm the value is the average of the two sub-hourly readings
    actual_value = float(now_point_rows.iloc[0]['dataValue'])
    assert abs(actual_value - expected_average) < 0.0001, (
        f"Expected now-point value {expected_average}, got {actual_value}"
    )

    # Confirm timeGenerated is recent (within the last minute), not a historical timestamp
    ingestion_time = now_point_rows.iloc[0]['timeGenerated']
    age = abs(datetime.now(timezone.utc) - ingestion_time)
    assert age < timedelta(minutes=1), (
        f"Expected timeGenerated to be close to now, but was {ingestion_time}"
    )

    # Confirm the sub-hourly readings themselves did NOT make it into the series
    past_to_dt = result_df[result_df['timeVerified'] > to_dt]
    assert len(past_to_dt) == 0, (
        f"Sub-hourly readings past toDateTime should not appear in the series, found: {past_to_dt}"
    )