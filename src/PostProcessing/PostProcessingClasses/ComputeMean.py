# -*- coding: utf-8 -*-
# ComputeMean.py
# -------------------------------
# Created By: Anointiyae Beasley
# Created Date: 08/14/2026
# -------------------------------
"""Combine multiple station series into one mean series."""

from PostProcessing.IPostProcessing import IPostProcessing
from DataClasses import Series, TimeDescription, get_input_dataFrame, SeriesDescription
from ModelExecution.dspecParser import PostProcessCall
from exceptions import Semaphore_Data_Exception

import pandas as pd
import numpy as np


class ComputeMean(IPostProcessing):
    """
    Compute one mean value per timestamp using multiple input series.
    
    NOTE: The first series shared will be used as the template for the time description

    Processing for each timestamp:

    1. Collect values from every target input series.
    2. Remove missing values and each series' own sentinel value.
    3. If dropOutlierValues is true:
       a. Calculate the median of the remaining values.
       b. Calculate each value's absolute difference from the median.
       c. Remove values whose absolute deviation from the median is
          greater than or equal to thresholdDeviationFromMedian.
    4. Calculate the mean of the remaining values.
    5. Construct and return a new series with the computed mean values.

    Air-temperature example:

    {
        "call": "ComputeMean",
        "args": {
            "dropOutlierValues": false,
            "target_inKeys": [
                "series-one_air-temp_25",
                "series-two_air-temp_25",
                "series-three_air-temp_25",
                "series-four_air-temp_25"
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

    def post_process_data(self, preprocessedData: dict[str, Series], postProcessCall: PostProcessCall) -> dict[str, Series]:
        """
        Combine multiple station series into one mean series.

        Args:
            preprocessedData (dict[str, Series]): A dictionary where the keys are the dependent series' "outKey"
                mapped to their corresponding Series object before post processing. Ex:
                {
                    "DPD_12": Series,
                    "APD_12": Series,
                    ...
                }
            postProcessCall (PostProcessCall): The type of post processing the model requires. All models
                that use this post processing class will have "call": "ComputeMean" in their dspec.
        """
        args = postProcessCall.args

        target_keys = args.get("target_inKeys")
        out_key = args.get("outKey")
        drop_outliers = args.get("dropOutlierValues", False)

        self._validate_arguments(
            target_keys=target_keys,
            out_key=out_key,
            drop_outliers=drop_outliers,
            preprocessed_data=preprocessedData
        )

        # validate and cast the thresholdDeviationFromMedian argument if drop_outliers is true
        threshold = self._get_outlier_threshold(args=args, drop_outliers=drop_outliers)

        # get only the series values for each input series
        # rename is called to ensure each series is uniquely identified by their inKey
        # instead of duplicating "dataValue" for each series
        series_values_list = [
            self._get_series_values(preprocessedData[key]).rename(key)
            for key in target_keys
        ]

        # Each column represents one series and each row represents
        # one verified time. An outer join preserves timestamps
        # appearing in any input station series.
        all_series_df = (
            pd.concat(
                series_values_list,
                axis=1,
                join="outer"
            )
            .sort_index()
        )

        # compute the mean for each row
        mean_values = all_series_df.apply(
            self._compute_mean,
            axis=1,
            drop_outliers=drop_outliers,
            threshold=threshold
        )

        output_df = self._build_output_dataframe(mean_values)

        # most metadata is left null because the computed series' metadata
        # may not represent the metadata of all the input series
        series_description = SeriesDescription(
            dataSource=None,
            dataSeries=out_key, # the computed series is identified by its outKey,
            dataLocation=None,
            dataDatum=None
        )

        # after validation, all input series will have the same fromDateTime, toDateTime, and interval,
        # so we can use the first series' time description as the template for the computed series
        time_template = preprocessedData[target_keys[0]].timeDescription
        time_description = TimeDescription(
            fromDateTime=time_template.fromDateTime,
            toDateTime=time_template.toDateTime,
            interval=time_template.interval,
            stalenessOffset=None
        )

        output_series = Series(series_description, time_description)

        output_series.dataFrame = output_df

        preprocessedData[out_key] = output_series

        return preprocessedData

    def _validate_arguments(self, target_keys: list[str] | None, out_key: str | None, drop_outliers: bool, preprocessed_data: dict[str, Series]) -> None:
        """Validate the ComputeMean configuration."""

        if not target_keys:
            raise ValueError("ComputeMean requires at least one target input series.")

        if not out_key:
            raise ValueError("ComputeMean requires an outKey to identify the computed series.")

        if not isinstance(drop_outliers, bool):
            raise TypeError("dropOutlierValues must be a boolean.")

        missing_keys = [key for key in target_keys if key not in preprocessed_data]
        if missing_keys:
            raise KeyError(
                "ComputeMean could not find these target series: "
                f"{missing_keys}"
            )

        # ensure all series have the same number of timestamps and the same time description
        template_series = preprocessed_data[target_keys[0]]
        for key in target_keys:
            series = preprocessed_data[key]
            # check df lengths
            if len(series.dataFrame) != len(template_series.dataFrame):
               raise ValueError(f"ComputeMean: Series: {key} has a different number of timestamps than the other series.")

            # check time descriptions
            if not self._compare_time_descriptions(series.timeDescription, template_series.timeDescription):
                raise ValueError(f"ComputeMean: Series: {key} has a different time description than the other series.")


    def _compare_time_descriptions(self, td1: TimeDescription, td2: TimeDescription) -> bool:
        """
        compares 2 time descriptions while ignoring the staleness offset since we specifically check
        equality for fromDateTime, toDateTime, and interval.

        Args:
            td1 (TimeDescription): The first time description to compare.
            td2 (TimeDescription): The second time description to compare.

        Returns:
            bool: True if the time descriptions are equal, False otherwise.
        """
        return (
            td1.fromDateTime == td2.fromDateTime and
            td1.toDateTime == td2.toDateTime and
            td1.interval == td2.interval
        )

    def _get_outlier_threshold(self, args: dict, drop_outliers: bool) -> float | None:
        """
        Validates and casts thresholdDeviationFromMedian to a float if drop_outliers is true.

        Args:
            args (dict): The arguments dictionary from the post process call
            drop_outliers (bool): Whether or not to drop outlier values
        
        Returns:
            float | None: The threshold value for outlier detection if drop_outliers is true, otherwise None.
        """
        # skip if drop_outliers is false
        if not drop_outliers:
            return None

        # ensure a threshold value was provided
        if "thresholdDeviationFromMedian" not in args:
            raise ValueError("thresholdDeviationFromMedian is required when dropOutlierValues is true.")

        # cast to float
        try:
            threshold = float(args["thresholdDeviationFromMedian"])
        except (TypeError, ValueError) as error:
            raise ValueError("thresholdDeviationFromMedian must be numeric.") from error

        # ensure threshold is not negative or 0
        if threshold <= 0:
            raise ValueError("thresholdDeviationFromMedian must be greater than 0.")

        return threshold

    def _build_output_dataframe(self, mean_values: pd.Series) -> pd.DataFrame:
        """
        Build a DataFrame using Semaphore's expected input columns.

        Args:
            mean_values (pd.Series): A pandas Series with verified time as the index and the computed mean value
                for each timestamp
        
        Returns:
            pd.DataFrame: A DataFrame with the expected input columns and the computed mean values.

        NOTE: Metadata is intentionally left null because this computed series
        is used only as part of the model input vector.
        """
        output_df = get_input_dataFrame()
        output_df["timeVerified"] = (mean_values.index)
        output_df["dataValue"] = (mean_values.astype(float).astype(str).to_numpy())
        output_df["dataUnit"] = None
        output_df["timeGenerated"] = None
        output_df["latitude"] = None
        output_df["longitude"] = None

        return output_df

    def _compute_mean(self, row: pd.Series, drop_outliers: bool, threshold: float | None) -> float | None:
        """
        Computes the mean value for a specific row. Raises an exception if all values are dropped due to being NaN or outliers.

        Args:
            row (pd.Series): A row of values from a dataframe where each column represents a different series
            drop_outliers (bool): Whether or not to drop outlier values
            threshold (float | None): The threshold value for outlier detection if drop_outliers is true, otherwise None
        
        Returns:
            float | None: The mean value for the row, or None if no valid values exist in the row

        NOTE: Sentinel values have already been converted to
        NaN before this method is called.
        """
        valid_values = row.dropna()

        if valid_values.empty:
            raise Semaphore_Data_Exception(f"ComputeMean: all values for timestamp: {row.name} were dropped due to being NaN.")

        if drop_outliers:
            if threshold is None:
                raise ValueError("An outlier threshold is required when drop_outliers is true.")

            median_value = valid_values.median()

            deviation_from_median = (valid_values - median_value).abs()

            # drop values at or above the threshold
            valid_values = valid_values[deviation_from_median < threshold]

        if valid_values.empty:
            raise Semaphore_Data_Exception(f"ComputeMean: all values for timestamp: {row.name} were dropped due to being outliers or NaN.")

        return valid_values.mean()

    def _get_series_values(self, series: Series) -> pd.Series:
        """
        This function gets all the values for a given series and replaces
        the series' sentinel values with NaN.

        Args:
            series (Series): The Series object to extract values from
        
        Returns:
            pd.Series: A pandas Series with verified time as the index and data values
                with the sentinel values replaced by NaN.
        """
        # index by verified time and get all values for this series
        series_values = series.dataFrame.set_index("timeVerified")["dataValue"]

        # if the sentinel is None, we skip the nan masking and 
        # replacement step and just return the series values as numeric
        if series.sentinelValue is not None:
            # convert sentinel to string
            # sentinel values that are already strings will not be affected
            sentinel_value = str(series.sentinelValue)

            # replaces sentinel values with NaN so that they are ignored in the mean calculation
            series_values = series_values.mask(
                series_values.astype(str) == sentinel_value,
                np.nan,
            )

        # cast the series values to numeric
        # nan is considered a float, so to_numeric will leave nan values as nan
        try:
            series_values = pd.to_numeric(series_values)
        except ValueError:
            raise ValueError(
                f"ComputeMean could not convert series: {series.description.dataSeries}'s "
                "dataValue column to numeric. Ensure the series contains only numeric values or the series' sentinel value."
            )

        return series_values