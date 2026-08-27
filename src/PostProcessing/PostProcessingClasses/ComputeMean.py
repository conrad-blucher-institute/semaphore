# -*- coding: utf-8 -*-
# ComputeMean.py
# -------------------------------
# Created By: Anointiyae Beasley
# Created Date: 08/14/2026
# -------------------------------

"""Combine multiple station series into one mean series."""

from copy import deepcopy

import pandas as pd

from PostProcessing.IPostProcessing import IPostProcessing
from DataClasses import Series, get_input_dataFrame
from ModelExecution.dspecParser import PostProcessCall


class ComputeMean(IPostProcessing):
    """
    Compute one mean value per timestamp using multiple input series.
    The first series shared will be used as the template for the time and series description.

    Processing for each timestamp:

    1. Collect values from every target input series.
    2. Remove missing values and each series' own sentinel value.
    3. If dropOutlierValues is true:
       a. Calculate the median of the remaining values.
       b. Calculate each value's absolute difference from the median.
       c. Remove values whose absolute deviation from the median is
          greater than or equal to thresholdDeviationFromMedian.
    4. Calculate the mean of the remaining values.
    5. If no values remain, fail the model.

    Air-temperature example:

    {
        "call": "ComputeMean",
        "args": {
            "dropOutlierValues": false,
            "target_inKeys": [
                "station-one_air-temp_25",
                "station-two_air-temp_25",
                "station-three_air-temp_25",
                "station-four_air-temp_25"
            ],
            "outKey": "combined-air-temp"
        }
    }

    Water-temperature example:

    {
        "call": "ComputeMean",
        "args": {
            "dropOutlierValues": true,
            "thresholdDeviationFromMedian": 3.5,
            "target_inKeys": [
                "Aransas-Wildlife-Refuge_water-temp_25",
                "Port-OConnor_water-temp_25",
                "Seadrift_water-temp_25",
                "Port-Lavaca_water-temp_25"
            ],
            "outKey": "esb-combined-water-temp"
        }
    }
    """

    def post_process_data( self,preprocessedData: dict[str, Series],postProcessCall: PostProcessCall) -> dict[str, Series]:
        """Combine multiple station series into one mean series."""
        args = postProcessCall.args

        target_keys = args.get("target_inKeys")
        out_key = args.get("outKey")
        drop_outliers = args.get(
            "dropOutlierValues",
            False,
        )

        self._validate_arguments(target_keys=target_keys,out_key=out_key,drop_outliers=drop_outliers,preprocessed_data=preprocessedData)

        threshold = self._get_outlier_threshold( args=args, drop_outliers=drop_outliers )

        target_series = [ preprocessedData[key] for key in target_keys]

        template_series = target_series[0]

        station_value_columns = [self.get_station_values(key,series) for key, series in zip(target_keys,target_series ) ]

        # Each column represents one station and each row represents
        # one verification timestamp. An outer join preserves timestamps
        # appearing in any input station series.
        values_by_time = (
            pd.concat(
                station_value_columns,
                axis=1,
                join="outer",
            )
            .sort_index()
        )

        mean_values = values_by_time.apply( self.compute_mean_for_timestamp,axis=1,drop_outliers=drop_outliers,threshold=threshold)
        missing_timestamps = mean_values.index[ mean_values.isna()]

        if not missing_timestamps.empty:
            raise ValueError(
                "ComputeMean failed because no valid station values remained "
                f"for these timestamps: {missing_timestamps.tolist()}"
            )

        output_df = self._build_output_dataframe(mean_values)

        series_description = deepcopy(template_series.description)

        # The computed series is identified by its outKey. 
        series_description.dataSeries = out_key

        # It does not inherit it's datum
        if hasattr(series_description,"dataDatum",):
            series_description.dataDatum = None

        time_description = deepcopy( template_series.timeDescription )

        output_series = Series(series_description, time_description)

        output_series.dataFrame = output_df

        preprocessedData[out_key] = output_series

        return preprocessedData

    def _validate_arguments(self, target_keys: list[str] | None,out_key: str | None,drop_outliers: bool,preprocessed_data: dict[str, Series]) -> None:
        """Validate the ComputeMean configuration."""
        if not target_keys:
            raise ValueError(
                "ComputeMean requires at least one target input series."
            )

        if not out_key:
            raise ValueError(
                "ComputeMean requires an outKey."
            )

        if not isinstance( drop_outliers, bool ):
            raise TypeError(
                "dropOutlierValues must be true or false."
            )

        missing_keys = [key for key in target_keys if key not in preprocessed_data]

        if missing_keys:
            raise KeyError(
                "ComputeMean could not find these target series: "
                f"{missing_keys}"
            )

    def _get_outlier_threshold(  self, args: dict, drop_outliers: bool ) -> float | None:
        """Validate and return the outlier threshold."""
        if not drop_outliers:
            return None

        if "thresholdDeviationFromMedian" not in args:
            raise ValueError(
                "thresholdDeviationFromMedian is required when "
                "dropOutlierValues is true."
            )

        try:
            threshold = float(
                args["thresholdDeviationFromMedian"]
            )
        except (TypeError, ValueError) as error:
            raise ValueError(
                "thresholdDeviationFromMedian must be numeric."
            ) from error

        if threshold < 0:
            raise ValueError(
                "thresholdDeviationFromMedian cannot be negative."
            )

        return threshold

    def _build_output_dataframe(self, mean_values: pd.Series) -> pd.DataFrame:
        """
        Build a DataFrame using Semaphore's expected input columns.

        Metadata is intentionally left null because this computed series
        is used only as part of the model input vector.
        """
        output_df = get_input_dataFrame()
        output_df["timeVerified"] = (mean_values.index)
        output_df["dataValue"] = ( mean_values.astype(float).astype(str).to_numpy())
        output_df["dataUnit"] = None
        output_df["timeGenerated"] = pd.NaT
        output_df["latitude"] = None
        output_df["longitude"] = None

        return output_df

    def compute_mean_for_timestamp(self, station_values: pd.Series, drop_outliers: bool, threshold: float | None) -> float | None:
        """
        Compute one mean value for one timestamp.

        Sentinel values have already been converted to
        NaN before this method is called.
        """
        valid_values = station_values.dropna()

        if valid_values.empty:
            return None

        if drop_outliers:
            if threshold is None:
                raise ValueError(
                    "An outlier threshold is required when "
                    "drop_outliers is true."
                )

            median_value = valid_values.median()

            deviation_from_median = ( valid_values - median_value).abs()

            # Values at or above the threshold are removed.
            valid_values = valid_values[ deviation_from_median < threshold ]

        if valid_values.empty:
            return None

        return float( valid_values.mean())

    def get_station_values( self, key: str, series: Series) -> pd.Series:
        """
        Return one station's numeric values indexed by timeVerified.

        Replace the station's sentinel values with pandas NaN.
        """
        #Grab all of the stations values and index by timeVerified
        station_values = (series.dataFrame.set_index("timeVerified")["dataValue"] )

        #Convert sentinel to string for furutre comparison. Sentinel's that are already strings will not be affected.
        sentinel_value = str(series.sentinelValue)

        #Checks whether the station values are equal to the sentinel value and replaces them with NaN. This allows for the sentinel values to be ignored when calculating the mean.
        station_values = station_values.mask(
                station_values.astype(str) == sentinel_value,
                float("nan"),
            )
         #Makes the station values numeric so that they can be used in calculations.
        try:
            station_values = pd.to_numeric(station_values)
        except ValueError:
                raise ValueError(
                    f"ComputeMean could not convert station {key}'s "
                    "dataValue to numeric. Ensure the station series contains only numeric values or the station's sentinel value."
                )

        return station_values.rename(key)