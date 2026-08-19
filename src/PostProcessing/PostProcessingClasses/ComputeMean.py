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
from DataClasses import Series, get_output_dataFrame
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
          greater than thresholdDeviationFromMedian.
    4. Calculate the mean of the remaining values.
    5. If no values remain, omit the timestamp from the output.

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

    def compute_mean_for_timestamp(self, station_values: pd.Series, drop_outliers: bool, threshold: float | None) -> float | None:
        """
        Compute one mean value for one timestamp.

        Sentinel values have already been replaced with NaN before this
        method is called.
        """

        valid_values = station_values.dropna()

        if valid_values.empty:
            return None

        if drop_outliers:
            median_value = valid_values.median()

            deviation_from_median = (
                valid_values - median_value
            ).abs()

            # Drop only values whose deviation is greater than the
            # threshold. Values equal to the threshold remain.
            valid_values = valid_values[
                deviation_from_median <= threshold
            ]

        if valid_values.empty:
            return None

        return float(valid_values.mean())

    def get_station_values(
        self,
        key: str,
        series: Series,
    ) -> pd.Series:
        """
        Return one station's numeric values indexed by timeVerified.

        The station's own sentinel value is replaced with NaN. Sentinels
        may be strings or integers, and different series may use different
        sentinel values.
        """

        input_df = series.dataFrame.copy()

        sentinel_value = series.sentinel_value

        if not isinstance(sentinel_value, (str, int)):
            raise TypeError(
                f"Series {key!r} sentinel value must be a str or int, "
                f"not {type(sentinel_value).__name__}."
            )

        raw_values = (
            input_df
            .set_index("timeVerified")["dataValue"]
        )

        numeric_values = pd.to_numeric(
            raw_values,
            errors="coerce",
        )

        numeric_sentinel = pd.to_numeric(
            pd.Series([sentinel_value]),
            errors="coerce",
        ).iloc[0]

        if pd.notna(numeric_sentinel):
            # Handles numeric sentinels stored in different forms:
            # 1000, "1000", or "1000.0".
            sentinel_mask = (
                numeric_values == numeric_sentinel
            )
        else:
            # Handles nonnumeric string sentinels such as "missing".
            sentinel_mask = (
                raw_values.astype(str) == str(sentinel_value)
            )

        station_values = numeric_values.mask(
            sentinel_mask
        )

        return station_values.rename(key)

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

        target_series = [preprocessedData[key] for key in target_keys]

        template_series = target_series[0]

        station_value_columns = [
            self.get_station_values(key, series)
            for key, series in zip(
                target_keys,
                target_series,
            )
        ]

        # Each column represents one station and each row represents one
        # verification time. The outer join retains timestamps appearing
        # in any of the input station series.
        values_by_time = pd.concat(
            station_value_columns,
            axis=1,
            join="outer",
        ).sort_index()

        output_values = values_by_time.apply(
            self.compute_mean_for_timestamp,
            axis=1,
            drop_outliers=drop_outliers,
            threshold=threshold,
        )

        # A None result means no station values remained for that
        # timestamp, so that entire output row is dropped.
        output_values = output_values.dropna()

        # Build the output using Semaphore's existing output structure.
        # timeVerified is stored as the DataFrame index because
        # get_output_dataFrame() does not include it as a column.
        output_df = get_output_dataFrame()
        output_df.index = output_values.index
        output_df.index.name = "timeVerified"

        # Semaphore requires dataValue to be stored as a string.
        output_df["dataValue"] = output_values.astype(str)

        # Copy metadata from the first input series and align it with the
        # timestamps that remain in the output.
        template_df = (
            template_series.dataFrame
            .copy()
            .set_index("timeVerified")
            .reindex(output_values.index)
        )

        for column in [
            "dataUnit",
            "timeGenerated",
            "leadTime",
        ]:
            if column in template_df.columns:
                output_df[column] = template_df[column]

        series_description = deepcopy(
            template_series.description
        )
        series_description.dataSeries = out_key

        time_description = deepcopy(
            template_series.timeDescription
        )

        output_series = Series(
            series_description,
            time_description,
        )
        output_series.dataFrame = output_df

        # No sentinel values are placed in output_df because timestamps
        # with no valid values are omitted. The metadata sentinel is
        # inherited from the template series for downstream processing.
        output_series.sentinel_value = (
            template_series.sentinel_value
        )

        preprocessedData[out_key] = output_series

        return preprocessedData