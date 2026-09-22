# -*- coding: utf-8 -*-
#test_PandasInterpolation.py
#-------------------------------
# Created By: Beto Estrada & Anointiyae Beasley  
# Created Date: 5/14/2024
# version 2.0
#----------------------------------
"""This file tests the Interpolation method and the other methods within it

run: docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_PandasInterpolation.py
 """ 
#----------------------------------
# 
#Imports
import sys
sys.path.append('/app/src')

import pytest
from datetime import datetime, timedelta, timezone

from src.DataClasses import get_input_dataFrame, Series, SeriesDescription, TimeDescription, DataIntegrityDescription
from src.DataIntegrity.IDataIntegrity import data_integrity_factory
from pandas import DataFrame
from exceptions import Semaphore_Data_Exception


dependent_series = {
            "_name": "Wind Direction",
            "location": "packChan",
            "source": "NOAATANDC",
            "series": "dWnDir",
            "unit": "degrees",
            "interval": 3600,
            "range": [0, 10],
            "datum": None,
            "dataIntegrityCall": {
                "call": "PandasInterpolation",
                "args": {
                    "method": "time",
                    "limit": '7200',
                    "limit_area":"inside" 
                }
            },
            "outKey": "WindDir_01",
            "verificationOverride": None
        }

testTimeDescription = TimeDescription(datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=6, tzinfo=timezone.utc),  timedelta(seconds = 3600))


df_seven_hour_series_missing_one = get_input_dataFrame()
df_seven_hour_series_missing_one.loc[0] = ['0.60', 'test', datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_one.loc[1] = ['0.66', 'test', datetime(2024, 1, 1, hour=1, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_one.loc[2] = ['0.69', 'test', datetime(2024, 1, 1, hour=2, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_one.loc[3] = ['0.72', 'test', datetime(2024, 1, 1, hour=4, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_one.loc[4] = ['0.76', 'test', datetime(2024, 1, 1, hour=5, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_one.loc[5] = ['0.79', 'test', datetime(2024, 1, 1, hour=6, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]

df_seven_hour_series_missing_three_consecutive = get_input_dataFrame()
df_seven_hour_series_missing_three_consecutive.loc[0] = ['0.60', 'test', datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_three_consecutive.loc[1] = ['0.66', 'test', datetime(2024, 1, 1, hour=1, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_three_consecutive.loc[2] = ['0.69', 'test', datetime(2024, 1, 1, hour=2, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_three_consecutive.loc[3] = ['0.79', 'test', datetime(2024, 1, 1, hour=6, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]

df_seven_hour_series_missing_one_tails_missing = get_input_dataFrame()
df_seven_hour_series_missing_one_tails_missing.loc[1] = ['0.66', 'test', datetime(2024, 1, 1, hour=1, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_one_tails_missing.loc[2] = ['0.69', 'test', datetime(2024, 1, 1, hour=2, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_one_tails_missing.loc[3] = ['0.72', 'test', datetime(2024, 1, 1, hour=4, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_one_tails_missing.loc[4] = ['0.76', 'test', datetime(2024, 1, 1, hour=5, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]


@pytest.mark.parametrize("dependent_series, timeDescription, inputs, expected_length_of_data", [
    (dependent_series, testTimeDescription, df_seven_hour_series_missing_one, 7), # One value missing, expects len of 7, no NaNs
    (dependent_series, testTimeDescription, df_seven_hour_series_missing_one_tails_missing, 5) # One value missing in middle, 2 missing at tails, expects len of 5, tails should be ignored
])
def test_interpolate_series(dependent_series: list, timeDescription: TimeDescription, inputs: DataFrame, expected_length_of_data: int):
    seriesDescription = SeriesDescription(
        dependent_series["source"],
        dependent_series["series"],
        dependent_series["location"],
        dataIntegrityDescription= DataIntegrityDescription(
            dependent_series["dataIntegrityCall"]['call'],
            dependent_series["dataIntegrityCall"]['args']
        )
    )
    
    inSeries = Series(description = seriesDescription, timeDescription = timeDescription)

    inSeries.dataFrame = inputs

    data_integrity_class = data_integrity_factory(seriesDescription.dataIntegrityDescription.call)
    outSeries = data_integrity_class.exec(inSeries)

    actual_length_of_data = len(outSeries.dataFrame)

    assert actual_length_of_data == expected_length_of_data


# A sample that doesn't land on the required hourly grid (e.g. a 6-minute NOAA
# reading between two hourly marks) mixed into a series that's also missing one
# of its required on-grid hours entirely (hour=3).
df_seven_hour_series_with_off_grid_sample_and_gap = get_input_dataFrame()
df_seven_hour_series_with_off_grid_sample_and_gap.loc[0] = ['0.60', 'test', datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_off_grid_sample_and_gap.loc[1] = ['0.66', 'test', datetime(2024, 1, 1, hour=1, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_off_grid_sample_and_gap.loc[2] = ['99.99', 'test', datetime(2024, 1, 1, hour=1, minute=30, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_off_grid_sample_and_gap.loc[3] = ['0.69', 'test', datetime(2024, 1, 1, hour=2, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
# hour=3 intentionally absent -- required by the dspec's grid, but missing from the provided data
df_seven_hour_series_with_off_grid_sample_and_gap.loc[4] = ['0.72', 'test', datetime(2024, 1, 1, hour=4, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_off_grid_sample_and_gap.loc[5] = ['0.76', 'test', datetime(2024, 1, 1, hour=5, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_off_grid_sample_and_gap.loc[6] = ['0.79', 'test', datetime(2024, 1, 1, hour=6, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]

def test_interpolate_series_keeps_union_of_required_and_provided_timestamps():

    seriesDescription = SeriesDescription(
        dependent_series["source"],
        dependent_series["series"],
        dependent_series["location"],
        dataIntegrityDescription= DataIntegrityDescription(
            dependent_series["dataIntegrityCall"]['call'],
            dependent_series["dataIntegrityCall"]['args']
        )
    )

    inSeries = Series(description = seriesDescription, timeDescription = testTimeDescription)
    inSeries.dataFrame = df_seven_hour_series_with_off_grid_sample_and_gap

    data_integrity_class = data_integrity_factory(seriesDescription.dataIntegrityDescription.call)
    outSeries = data_integrity_class.exec(inSeries)

    # Union of the 7 required hourly timestamps and the 1 off-grid provided timestamp
    assert len(outSeries.dataFrame) == 8

    off_grid_timestamp = datetime(2024, 1, 1, hour=1, minute=30, tzinfo=timezone.utc)
    output_by_time = dict(zip(outSeries.dataFrame['timeVerified'], outSeries.dataFrame['dataValue']))

    # The off-grid sample must survive, with its original value untouched
    assert off_grid_timestamp in output_by_time
    assert output_by_time[off_grid_timestamp] == '99.99'

    # The required-but-missing on-grid hour must still get interpolated
    missing_hour = datetime(2024, 1, 1, hour=3, tzinfo=timezone.utc)
    assert missing_hour in output_by_time
    assert output_by_time[missing_hour] is not None


def build_series(inputs: DataFrame, timeDescription: TimeDescription, argOverrides: dict = None) -> Series:
    """Builds the series the interpolation class expects from one of the input dataframes above.
    argOverrides replaces individual dataIntegrityCall args, so a test can vary limit_area or limit
    without needing a second copy of the dependent series definition."""
    args = dict(dependent_series["dataIntegrityCall"]['args'])
    if argOverrides is not None:
        args.update(argOverrides)

    seriesDescription = SeriesDescription(
        dependent_series["source"],
        dependent_series["series"],
        dependent_series["location"],
        dataIntegrityDescription= DataIntegrityDescription(
            dependent_series["dataIntegrityCall"]['call'],
            args
        )
    )

    inSeries = Series(description = seriesDescription, timeDescription = timeDescription)
    inSeries.dataFrame = inputs
    return inSeries


# An off grid sample at 1:30 with the required 2:00 hour missing entirely. The value interpolated for
# 2:00 differs depending on whether the interpolation respects the real spacing of the timestamps,
# which is what makes this series able to tell 'time' and 'linear' apart.
df_seven_hour_series_with_unevenly_spaced_gap = get_input_dataFrame()
df_seven_hour_series_with_unevenly_spaced_gap.loc[0] = ['0.60', 'test', datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_unevenly_spaced_gap.loc[1] = ['0.66', 'test', datetime(2024, 1, 1, hour=1, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_unevenly_spaced_gap.loc[2] = ['0.90', 'test', datetime(2024, 1, 1, hour=1, minute=30, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
# hour=2 intentionally absent -- it sits 30 minutes after the off grid sample but 60 minutes before the next value
df_seven_hour_series_with_unevenly_spaced_gap.loc[3] = ['0.69', 'test', datetime(2024, 1, 1, hour=3, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_unevenly_spaced_gap.loc[4] = ['0.72', 'test', datetime(2024, 1, 1, hour=4, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_unevenly_spaced_gap.loc[5] = ['0.76', 'test', datetime(2024, 1, 1, hour=5, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_unevenly_spaced_gap.loc[6] = ['0.79', 'test', datetime(2024, 1, 1, hour=6, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]


def test_interpolate_series_weights_by_time_not_by_row_position():
    """The interpolated value must be weighted by how far apart the timestamps actually are. Treating
    the rows as evenly spaced, which is what pandas' 'linear' method does, would produce 0.795 here."""

    inSeries = build_series(df_seven_hour_series_with_unevenly_spaced_gap, testTimeDescription)

    data_integrity_class = data_integrity_factory(inSeries.description.dataIntegrityDescription.call)
    outSeries = data_integrity_class.exec(inSeries)

    output_by_time = dict(zip(outSeries.dataFrame['timeVerified'], outSeries.dataFrame['dataValue']))

    # 2:00 sits 30 minutes after 1:30 (0.90) and 60 minutes before 3:00 (0.69), so it lands a third
    # of the way between them rather than half way
    missing_hour = datetime(2024, 1, 1, hour=2, tzinfo=timezone.utc)
    assert float(output_by_time[missing_hour]) == pytest.approx(0.83)


# The scenario this class exists for: a source that reports every 6 minutes but happens to be missing
# the top of the hour, which is the only timestamp the model's grid actually requires. The samples at
# 2:54 and 3:06 bookend the missing 3:00 by 12 minutes, so it is safely interpolated even though the
# limit is only an hour.
df_seven_hour_series_with_six_minute_samples_around_a_missing_hour = get_input_dataFrame()
df_seven_hour_series_with_six_minute_samples_around_a_missing_hour.loc[0] = ['0.60', 'test', datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_six_minute_samples_around_a_missing_hour.loc[1] = ['0.66', 'test', datetime(2024, 1, 1, hour=1, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_six_minute_samples_around_a_missing_hour.loc[2] = ['0.69', 'test', datetime(2024, 1, 1, hour=2, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_six_minute_samples_around_a_missing_hour.loc[3] = ['0.70', 'test', datetime(2024, 1, 1, hour=2, minute=54, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
# hour=3 intentionally absent -- the required grid point the source failed to report
df_seven_hour_series_with_six_minute_samples_around_a_missing_hour.loc[4] = ['0.80', 'test', datetime(2024, 1, 1, hour=3, minute=6, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_six_minute_samples_around_a_missing_hour.loc[5] = ['0.72', 'test', datetime(2024, 1, 1, hour=4, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_six_minute_samples_around_a_missing_hour.loc[6] = ['0.76', 'test', datetime(2024, 1, 1, hour=5, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_with_six_minute_samples_around_a_missing_hour.loc[7] = ['0.79', 'test', datetime(2024, 1, 1, hour=6, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]


def test_interpolate_series_fills_a_grid_point_from_nearby_off_grid_samples():
    """A missing grid point whose nearest real values are only minutes away must be interpolated, even
    though the two surrounding grid points are further apart than the limit allows."""

    inSeries = build_series(df_seven_hour_series_with_six_minute_samples_around_a_missing_hour, testTimeDescription)

    data_integrity_class = data_integrity_factory(inSeries.description.dataIntegrityDescription.call)
    outSeries = data_integrity_class.exec(inSeries)

    output_by_time = dict(zip(outSeries.dataFrame['timeVerified'], outSeries.dataFrame['dataValue']))

    # 3:00 is half way between 2:54 (0.70) and 3:06 (0.80)
    missing_hour = datetime(2024, 1, 1, hour=3, tzinfo=timezone.utc)
    assert float(output_by_time[missing_hour]) == pytest.approx(0.75)


# Two consecutive required hours missing. The values bookending the run are 2:00 and 5:00, three hours
# apart, which is more than the 7200 limit allows.
df_seven_hour_series_missing_two_consecutive = get_input_dataFrame()
df_seven_hour_series_missing_two_consecutive.loc[0] = ['0.60', 'test', datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_two_consecutive.loc[1] = ['0.66', 'test', datetime(2024, 1, 1, hour=1, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_two_consecutive.loc[2] = ['0.69', 'test', datetime(2024, 1, 1, hour=2, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
# hours 3 and 4 intentionally absent
df_seven_hour_series_missing_two_consecutive.loc[3] = ['0.76', 'test', datetime(2024, 1, 1, hour=5, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_two_consecutive.loc[4] = ['0.79', 'test', datetime(2024, 1, 1, hour=6, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]

# The first three required hours missing. There is no value before the run, so the check treats the
# window as starting one interval earlier.
df_seven_hour_series_missing_three_leading = get_input_dataFrame()
# hours 0, 1 and 2 intentionally absent
df_seven_hour_series_missing_three_leading.loc[0] = ['0.69', 'test', datetime(2024, 1, 1, hour=3, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_three_leading.loc[1] = ['0.72', 'test', datetime(2024, 1, 1, hour=4, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_three_leading.loc[2] = ['0.76', 'test', datetime(2024, 1, 1, hour=5, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_three_leading.loc[3] = ['0.79', 'test', datetime(2024, 1, 1, hour=6, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]

# The last three required hours missing, the mirror of the case above.
df_seven_hour_series_missing_three_trailing = get_input_dataFrame()
df_seven_hour_series_missing_three_trailing.loc[0] = ['0.60', 'test', datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_three_trailing.loc[1] = ['0.66', 'test', datetime(2024, 1, 1, hour=1, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_three_trailing.loc[2] = ['0.69', 'test', datetime(2024, 1, 1, hour=2, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
df_seven_hour_series_missing_three_trailing.loc[3] = ['0.72', 'test', datetime(2024, 1, 1, hour=3, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
# hours 4, 5 and 6 intentionally absent


@pytest.mark.parametrize("inputs, argOverrides", [
    (df_seven_hour_series_missing_two_consecutive, None), # bookended by 2:00 and 5:00, 3 hours apart
    (df_seven_hour_series_missing_three_consecutive, None), # bookended by 2:00 and 6:00, 4 hours apart
    (df_seven_hour_series_missing_three_leading, {'limit_area': 'None'}), # edge gap, but extrapolation is allowed so it is checked
    (df_seven_hour_series_missing_three_trailing, {'limit_area': 'None'}), # edge gap, but extrapolation is allowed so it is checked
])
def test_interpolate_series_rejects_gaps_over_the_limit(inputs: DataFrame, argOverrides: dict):

    inSeries = build_series(inputs, testTimeDescription, argOverrides)

    data_integrity_class = data_integrity_factory(inSeries.description.dataIntegrityDescription.call)

    with pytest.raises(Semaphore_Data_Exception):
        data_integrity_class.exec(inSeries)


@pytest.mark.parametrize("inputs, expected_length_of_data", [
    (df_seven_hour_series_missing_three_leading, 4), # edge gap is ignored, the leading NaNs are simply dropped
    (df_seven_hour_series_missing_three_trailing, 4) # edge gap is ignored, the trailing NaNs are simply dropped
])
def test_interpolate_series_ignores_edge_gaps_when_extrapolation_is_not_allowed(inputs: DataFrame, expected_length_of_data: int):
    """With limit_area 'inside' the edge NaNs are never filled, so an oversized gap there is not the
    interpolator's problem -- the rows are dropped and data validation decides whether that is fatal."""

    inSeries = build_series(inputs, testTimeDescription)

    data_integrity_class = data_integrity_factory(inSeries.description.dataIntegrityDescription.call)
    outSeries = data_integrity_class.exec(inSeries)

    assert len(outSeries.dataFrame) == expected_length_of_data
