# -*- coding: utf-8 -*-
# statistics.py
#----------------------------------
# Created By : Christian Quintero
# Created Date: 04/20/2026
# version 1.0
#----------------------------------
"""
This file contains the Statistics class which provides methods for calculating
various metrics such as percentiles, mean, standard deviation (std_dev), and min/max for
a given dataset.
""" 
#----------------------------------
import numpy as np


class Statistics:
    # members
    PERCENTILES = [1, 5, 10, 25, 50, 75, 90, 95, 99]

    def compute_statistics(self, data: np.ndarray | list[float | int]) -> dict[str, float]:
        '''
        Computes the following statistics for the given data:
        - Percentiles in the PERCENTILES list
        - Minimum value
        - Maximum value
        - Mean value (aka average)
        - Standard deviation (std_dev)

        NOTE: The 50th percentile is the same as the median, so we do not compute it separately.

        :param data: np.ndarray | list[float | int] - The input data to compute statistics on.
            It can be a numpy ndarray or a list of floats/ints. If a multidimensional array is provided, it will
            be flattened to a 1D list regardless of its original shape.

        :returns dict[str, float] - A dictionary with the computed statistics such as
            {
                'p1': 1,
                'p5': 5,
                'p10': 10,
                ...
                'min': 0,
                'max': 100,
                'mean': 1,
                'std_dev': 0.5,
            }
        '''
        # statistics dictionary to hold the results
        statistics = {}

        # flatten data to a 1D ndarray of floats
        flattened_data = np.asarray(data, dtype=float).flatten()

        if flattened_data.size == 0:
            raise ValueError("Cannot compute statistics on an empty dataset.")
        
        if np.any(np.isnan(flattened_data)):
            raise ValueError("Cannot compute statistics on a dataset containing NaN values.")
        
        # format percentiles as { 'p1': 1, 'p5': 5, ... }
        for p in self.PERCENTILES:
            statistics[f'p{p}'] = np.percentile(flattened_data, p)
        
        statistics['min'] = np.min(flattened_data)
        statistics['max'] = np.max(flattened_data)
        statistics['mean'] = np.mean(flattened_data)
        statistics['std_dev'] = np.std(flattened_data)

        # convert all values from numpy floats to regular floats
        for key in statistics:
            statistics[key] = float(statistics[key])

        return statistics