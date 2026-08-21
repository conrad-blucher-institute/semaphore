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
            "outKey": "combined-water-temp"
        }
    }
    """
    def post_process_data(self, preprocessedData: dict[str, Series], postProcessCall: PostProcessCall) -> dict[str, Series]:
        """Combine multiple station series into one mean series."""

        args = postProcessCall.args

        target_keys = args["target_inKeys"]
        out_key = args["outKey"]
        drop_outliers = args.get(
            "dropOutlierValues",
            False,
        )

        if not target_keys:
            raise ValueError(
                "ComputeMean requires at least one target input series."
            )

        missing_keys = [key for key in target_keys if key not in preprocessedData]

        if missing_keys:
            raise KeyError(
                "ComputeMean could not find these target series: "
                f"{missing_keys}"
            )

        if not isinstance(drop_outliers, bool):
            raise TypeError(
                "dropOutlierValues must be true or false."
            )

        threshold = None

        if drop_outliers:
            if "thresholdDeviationFromMedian" not in args:
                raise ValueError(
                    "thresholdDeviationFromMedian is required when "
                    "dropOutlierValues is true."
                )

            try:
                threshold = float(args["thresholdDeviationFromMedian"])
            except (TypeError, ValueError) as error:
                raise ValueError(
                    "thresholdDeviationFromMedian must be numeric."
                ) from error

            if threshold < 0:
                raise ValueError(
                    "thresholdDeviationFromMedian cannot be negative."
                )

        #For every key in target_keys, it retrieves the matching Series from preprocessedData and stores them in a list.
        target_series = [preprocessedData[key] for key in target_keys]

        # Used for copying metadata from the first input series to the output series.
        template_series = target_series[0]

        # Grabs each stations key and series and passes them to get_station_values to retrieve a Series of numeric values with the sentinel replaced by NaN. 
        # The resulting Series are stored in a list. 
        station_value_columns = [
            self.get_station_values(key, series)
            for key, series in zip(
                target_keys,
                target_series,
            )
        ]

        # Joins series beside eachother as columns. Each column represents one station and each row represents one
        # verification time. The outer join retains timestamps appearing
        # in any of the input station series. If a station does not have a timestamp, its value is NaN for that timestamp.
        values_by_time = pd.concat(station_value_columns, axis=1, join="outer" ).sort_index()

        # Calulate the mean for each timestamp, dropping outliers if requested. 
        mean_values = values_by_time.apply(self.compute_mean_for_timestamp, axis=1, drop_outliers=drop_outliers, threshold=threshold)

        missing_timestamps = mean_values.index[
            mean_values.isna()
        ]

        if not missing_timestamps.empty:
            raise ValueError(
                "ComputeMean failed because no valid station values remained "
                f"for these timestamps: {missing_timestamps.tolist()}"
            )
        
        # Creates empty df with Semaphore's expectec columns.
        output_df = get_input_dataFrame()

        # Add the timestamps and mean values in str formatto the output dataframe.
        output_df["timeVerified"] = mean_values.index
        output_df["dataValue"] = mean_values.to_numpy().astype(str)

        # Copy dataUnit, timeGenerated, leadTime from the first input series and align it with the
        # timestamps that remain in the output. 
        template_df = ( template_series.dataFrame .copy() .set_index("timeVerified") .reindex(mean_values.index))

        for column in ["dataUnit","timeGenerated","leadTime"]:
            if column in template_df.columns:
                output_df[column] = template_df[column].to_numpy()

        # Convert from datetime64[ns] to object so missing values can be
        # represented as Python None instead of pandas NaT.
        output_df["timeGenerated"] = (output_df["timeGenerated"].astype("object") )

        # Replace missing timeGenerated values with Python None.
        output_df.loc[ output_df["timeGenerated"].isnull(),  "timeGenerated"] = None

        series_description = deepcopy(
            template_series.description
        )
        series_description.dataSeries = out_key

        time_description = deepcopy(
            template_series.timeDescription
        )


        output_series = Series(series_description,time_description )
        output_series.dataFrame = output_df

        output_series.sentinelValue = template_series.sentinelValue
        

        preprocessedData[out_key] = output_series

        return preprocessedData

    def compute_mean_for_timestamp(self, station_values: pd.Series, drop_outliers: bool, threshold: float | None) -> float | None:
            """
            Compute one mean value for one timestamp.
    
            Sentinel values have already been replaced with NaN before this
            method is called.
            """

            #Drop NaN's
            valid_values = station_values.dropna()
    
            if valid_values.empty:
                return None
            # If drop_outliers is true, calculate the median and drop values whose absolute deviation from the median is greater than or equal to the threshold.
            if drop_outliers:
                median_value = valid_values.median()
    
                deviation_from_median = (
                    valid_values - median_value
                ).abs()
    
                # Drop values whose deviation is greater than or equal to the threshold.
                # Only values with deviation below the threshold remain.
                valid_values = valid_values[
                    deviation_from_median < threshold
                ]
    
            if valid_values.empty:
                return None
    
            return float(valid_values.mean())
    
    def get_station_values(self, key: str, series: Series ) -> pd.Series:
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
                station_values == sentinel_value,
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