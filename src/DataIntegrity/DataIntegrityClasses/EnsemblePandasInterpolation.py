# -*- coding: utf-8 -*-
# EnsemblePandasInterpolation.py
#----------------------------------
# Created By: Op Team
# Created Date: 5/15/2024
# version 1.0
#----------------------------------
"""This module uses pandas to interpolate ensemble data
 """ 
#----------------------------------
# 
#
#Imports
import pandas as pd
import numpy as np
from datetime import timedelta

from DataIntegrity.IDataIntegrity import IDataIntegrity
from DataClasses import Series
from utility import log



class EnsemblePandasInterpolation(IDataIntegrity):
    """
    This class interpolates data with pandas.

    args: 
            limit - The max gap distance that will be interpolated.
            method - The Pandas method to interpolate with.


    json_copy:
    "dataIntegrityCall": {
        "call": "PandasInterpolation",
        "args": {
            "limit":"",
            "method":""  
            "limit_area":"None | inside | outside"
        }
    }
    """

    def exec(self, inSeries: Series) -> Series: 
        """This method will interpolate the results from the query if the gaps between the NaNs are not larger than the limit
        specifically designed for ensemble data. 

        Args:
            inSeries (Series): The incomplete merged result of the DB and DI queries

        Returns:
            Series : The Series with new interpolated Inputs added
        """
        timeDescription = inSeries.timeDescription
        seriesDescription = inSeries.description
        dataIntegrityDescription = seriesDescription.dataIntegrityDescription
        
        # Will hard fail if one or both doesn't exist
        method = dataIntegrityDescription.args['method']
        limit = int(dataIntegrityDescription.args['limit'])
        limit_area = dataIntegrityDescription.args['limit_area']
        limit_area = None if limit_area == 'None' else limit_area
        input_df = inSeries.dataFrame.copy(deep=True)
        
        # The limit provided is in seconds, we need to know how many rows can be interpolated
        row_limit = int(timedelta(seconds = limit) // timeDescription.interval)

        # The ensemble data is in the dataValue row as a list of values, we want to interpolate by ensemble members.
        # So we convert the dataValue column to a list of lists, and then create a new dataframe with the ensemble members as columns.
        ensemble_data = input_df['dataValue'].to_list()
        input_df.set_index('timeVerified', inplace=True)
        df_ensemble_data = pd.DataFrame(ensemble_data, index=input_df.index).astype(float)

        # Create a continues index that will mark gaps with nans
        df_ensemble_data = df_ensemble_data.reindex(pd.date_range(start=df_ensemble_data.index[0], end=df_ensemble_data.index[-1], freq=timeDescription.interval))

        # We want to not interpolate if there are too many Nans in a row. However the pandas limit parameter only stops interpolation once its
        # counted a cumulative sum of Nans higher than limit. Thus it keeps the Nans in that group where the cumulative sum was still < limit.
        # This code creates a mask that is true for all rows that are part of a group of Nans that is larger than the limit, where the interpolation
        # error would occur.
        nan_mask = df_ensemble_data[0].isna()
        group_sizes = nan_mask.groupby((~nan_mask).cumsum()).transform('sum')
        error_mask = (nan_mask & group_sizes.gt(row_limit))

        # Interpolate the nans
        df_ensemble_interpolated = df_ensemble_data.interpolate(method= method, limit= row_limit, limit_area= limit_area)

        # Here we replace the mistakenly interpolated rows by replacing them with nan. Dropping nans remove the rows and keeps the df clean
        df_ensemble_interpolated[error_mask] = np.nan
        df_ensemble_interpolated.dropna(inplace=True)
        df_ensemble_interpolated.astype(str)

        # The dataframe has changed sizes so we reindex, add the interpolated values, and then ffill the metadata.
        out_df = input_df.reindex(df_ensemble_interpolated.index) 
        out_df['dataValue'] = df_ensemble_interpolated.values.tolist() 
        out_df.fillna(method='ffill', inplace=True)
        out_df.reset_index(inplace=True, names='timeVerified')

        outSeries = Series(seriesDescription, timeDescription)
        outSeries.dataFrame = out_df

        return outSeries