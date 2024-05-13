# -*- coding: utf-8 -*-
#test_Interpolation.py
#-------------------------------
# Created By: Anointiyae Beasley
# Created Date: 10/24/2023
# version 1.0
#----------------------------------
"""This file tests the Interpolation method and the other methods within it
 """ 
#----------------------------------
# 
#Imports
import pytest
from src.SeriesProvider.SeriesProvider import TimeDescription, Series, SeriesDescription, Input
from datetime import datetime, timedelta, timezone
import pandas as pd

def __interpolate_series(inSeries: Series) -> Series: 
         """This method will interpolate the results from the query if the gaps between the NaNs are not larger than the limit

        Args:
            inSeries (Series): The incomplete merged result of the DB and DI queries

        Returns:
            Series : The Series with new interpolated Inputs added
        """
         print("InSeries:", inSeries)
         
         timeDescription = inSeries.timeDescription

         seriesDescription = inSeries.description

         interpolationParameters = seriesDescription.interpolationParameters

         #What if its none?
         method = interpolationParameters.get("method")
         limit = timedelta(seconds= interpolationParameters.get("limit"))
         if limit is None:
            print(f'''Interpolation error,
                   Reason: limit not defined.\n 
                   {seriesDescription} \n 
                   {timeDescription} \n 
               ''')
            return inSeries
        
         input_df = pd.DataFrame([input.__dict__ for input in inSeries.data])
         
         input_df.set_index('timeVerified', inplace=True)
         
         # Add the missing timeVerified datetimes and fill the remaining columns with NaNs
         filled_input_df = __fill_in_date_gaps(input_df, timeDescription.fromDateTime, timeDescription.toDateTime, timeDescription.interval)
         
         # Rename the index back to 'timeVerified' after filling in date gaps
         filled_input_df.index = filled_input_df.index.rename('timeVerified')

         largerThanLimit = __check_gap_distance(filled_input_df, limit, timeDescription.interval)
         if largerThanLimit: 
            print(f'''Interpolation error,
                       Reason: There are gaps in the data that are larger than the limit parameter.\n 
                       {seriesDescription} \n 
                       {timeDescription} \n 
                   ''')
            return inSeries 

         # Cast dataValue string to float for interpolation
         filled_input_df['dataValue'] = filled_input_df['dataValue'].astype(float)

         # No limit is set since we already checked if the limit was passed above
         filled_input_df['dataValue'] = filled_input_df['dataValue'].interpolate(method= method, limit_direction="both")

         # Convert dataValue back to string
         filled_input_df['dataValue'] = filled_input_df['dataValue'].astype(str)

         # Reset the index to make timeVerified a normal column again
         filled_input_df.reset_index(inplace=True)

         # Backfill/Forward-fill the remaining columns that are NaN/NaT.
         filled_input_df.ffill().bfill()

         inputs = [] 
         for __, row in filled_input_df.iterrows():
             inputs.append(Input(
                 dataValue=row["dataValue"],
                 dataUnit=row["dataUnit"],
                 timeGenerated=row["timeGenerated"],
                 timeVerified=row["timeVerified"],
                 longitude=row["longitude"],
                 latitude=row["latitude"]
             ))
        
         outSeries = Series(seriesDescription, True, timeDescription)
         outSeries.data = inputs

         return outSeries
     
def  __check_gap_distance( df: pd.DataFrame, limit: timedelta, interval: timedelta) -> bool:
        """This method will remove all non-NaN rows from the DataFrame leaving just the NaN rows. 
        It will then check to see if there is a group of consecutive NaNs that are larger than the limit.

        Args:
            df (pd.DataFrame): The DataFrame of data
            limit (timedelta): The max gap distance the researcher allows for their model
            interval (timedelta): The time step separating the data points in order

        Returns:
            bool: Determines if a gap is larger than the limit
        """
        # Find any rows where dataValue is NaN
        nan_mask = df['dataValue'].isna()

        # Extract only rows with NaNs
        rows_with_nan = df[nan_mask]

        previous_date = None
        nan_gap_size = 1
        for current_date in rows_with_nan.index:

            # If first date, set the previous date to the current date and then continue
            if previous_date is None:

                previous_date = current_date

                continue

            time_difference = current_date - previous_date

            # Since there are only rows with NaNs present, if the time difference between the previous date and the current date is 
            # greater than the interval then there is a prediction/data present between the values
            if (time_difference > interval):
                # Reset the counter
                nan_gap_size = 1

            else:
                nan_gap_size += 1

            # If the NaN gap is greater than the limit, return True
            if(nan_gap_size * interval > limit):
                return True

            previous_date = current_date

        # If no NaN gap was greater than the limit
        return False
             
def __fill_in_date_gaps( df : pd.DataFrame , start_date : datetime , end_date : datetime , interval : timedelta ) -> pd.DataFrame:
        """Fills in missing date gaps with NaNs based on given interval. The DataFrame index must be of type datetime

            Args:
                df [DataFrame]: pandas DataFrame

                start_date (datetime): Date to start at

                end_date (datetime): Date to end at

                interval (timedelta): The time step separating the data points in order

            Returns:
                [DataFrame]: pandas DataFrame containing desired data
            """
        print("Dataframe:", df)
        all_dates = pd.date_range(start=start_date, end=end_date, freq=interval)
        all_dates_df = pd.DataFrame(index=all_dates)
        merged_df = pd.merge(all_dates_df, df, left_index=True, right_index=True, how='left')

        # Apply the name from dataUnit,longitude, and latitude to all rows with NaN values in merged_df
        merged_df["dataUnit"].fillna(df["dataUnit"].iloc[0], inplace=True)
        merged_df["longitude"].fillna(df["longitude"].iloc[0], inplace=True)
        merged_df["latitude"].fillna(df["latitude"].iloc[0], inplace=True)
        
        return merged_df




#mimicking the missing datapoints tht will be passed to be filled during interpolation
@pytest.fixture
def api_data():
    return {
        "data": {# Example data from 5/13/2024 7:20 pm to 4:20 am
            "series1": [
                #[1715628000, 10],   #19:20  
                #[1715631600, 15],   #20:20
                [1715635200, 12],   #21:20
                [1715638800, 13],  #22:20
                #[1715642400, 14],  #23:20
                #[1715646000, 16],   #00
                [1715649600, 17],  #01:20
                [1715653200, 18],   #02:20
                [1715656800, 19],
                [1715660400, 20]
            ]
        }
    }

@pytest.fixture
def dependent_series():
    return [
        {
            "_name": "Wind Direction",
            "location": "packChan",
            "source": "NOAATANDC",
            "series": "dWnDir",
            "unit": "degrees",
            "interval": 3600,
            "range": [0, 10],
            "datum": None,
            "interpolationParameters": {
                "method": "linear",
                "limit": 7200
            },
            "outKey": "WindDir_01",
            "verificationOverride": None
        }
    ]

@pytest.fixture
def interval():
    return 3600

def test_interpolation( api_data, dependent_series, interval):
    series_description = SeriesDescription(
        dependent_series[0]["source"],
        dependent_series[0]["series"],
        dependent_series[0]["location"],
        dependent_series[0]["datum"],
        dependent_series[0]["interpolationParameters"],
        dependent_series[0]["verificationOverride"]

    )

    time_description = TimeDescription(
        fromDateTime=datetime.fromtimestamp(1715628000, tz=timezone.utc),
        toDateTime=datetime.fromtimestamp(1715660400, tz=timezone.utc),
        interval=timedelta(seconds=interval)
    )
    
    print("dependent_series[0][interpolationParameters]:", dependent_series[0]["interpolationParameters"])

    inSeries = Series(
        description=series_description,
        isComplete=True,
        timeDescription=time_description
    )
    inputs = []
    counter = 0
    for timestamp, value in api_data["data"]["series1"]:
        
        inputs.append((
            Input(
                dataValue= value, 
                dataUnit= dependent_series[0]["unit"], 
                timeVerified = datetime.fromtimestamp(timestamp, tz=timezone.utc), 
                timeGenerated= datetime.fromtimestamp(timestamp, tz=timezone.utc),
                longitude=".98",
                latitude=".87"
               
            )))
        counter += 1       
        print("TIME VERIFIED", datetime.fromtimestamp(timestamp, tz=timezone.utc))                          

    inSeries.data = inputs

    print("inSeries.data", inSeries.data)
    outSeries = __interpolate_series(inSeries)
    print("outSeries.data:", outSeries.data)

    # Add assertions to validate the result of the interpolation process
    assert len(outSeries.data) > 0, "No data in the resulting outSeries."
    assert all(data_point.dataValue is not None for data_point in outSeries.data), "NaN values found in dataValue."
    assert outSeries.data[0].dataValue != outSeries.data[-1].dataValue, "First and last dataValue are equal."
