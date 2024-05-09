# -*- coding: utf-8 -*-
#SeriesProvider.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/30/2023
# version 2.0
#----------------------------------
"""This class holds is the start point for interacting with the data section of semaphore. All data requests should go through here.
 """ 
#----------------------------------
# 
#
#Imports
from SeriesStorage.ISeriesStorage import series_storage_factory
from DataIngestion.IDataIngestion import data_ingestion_factory
from DataClasses import Series, SemaphoreSeriesDescription, SeriesDescription, TimeDescription, Input
from utility import log
import pandas as pd
from datetime import timedelta,datetime



class SeriesProvider():

    def __init__(self) -> None:
        self.seriesStorage = series_storage_factory()
    
    def save_series(self, series: Series) -> Series:
        """Passes a series to Series Storage to be stored.
            :param series - The series to store.
            :returns series - A series containing only the stored values.
        """
        returningSeries = Series(series.description, True)

        if not (type(series.description) == SemaphoreSeriesDescription): #Check and make sure this is actually something with the proper description to be inserted
            returningSeries.isComplete = False
            returningSeries.nonCompleteReason = f'A save Request must be provided a series with a SemaphoreSeriesDescription not a Series Description'
        else:
            returningSeries = self.seriesStorage.insert_output(series)

        return returningSeries
    
    def request_input(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series:
        """This method attempts to return a series matching a series description and a time description.
            It will attempt first to get the series from series storage, kicking off data ingestion if series storage
            doesn't have all the data.
            NOTE: If an interval is not provided in the time description, the interval will be assumed to be 6 minutes
            :param seriesDescription - A description of the wanted series.
            :param timeDescription - A description of the temporal information of the wanted series. 
            :returns series - The series containing as much data as could be found.
        """
        log(f'Init input request from {seriesDescription}|{timeDescription}')
        
        # If an interval was not provided we have to make an assumption to be able to validate it. Here we assume the interval to be 6 minuets
        timeDescription.interval = timedelta(minutes=6) if timeDescription.interval == None else timeDescription.interval
        
        # Pull data from series storage and validate it, if valid return it
        log(f'Init DB Query from {seriesDescription}|{timeDescription}')
        series_storage_results = self.seriesStorage.select_input(seriesDescription, timeDescription)
        validated_series_storage_results = self.__generate_resulting_series(seriesDescription, timeDescription, series_storage_results.data)
        if(validated_series_storage_results.isComplete):
            return validated_series_storage_results

        # If series storage results weren't valid
        # Pull data ingestion, validate it with the series storage results, if valid return it
        log(f'Init DI Query from {seriesDescription}|{timeDescription}')
        data_ingestion_class = data_ingestion_factory(seriesDescription)
        data_ingestion_results = data_ingestion_class.ingest_series(seriesDescription, timeDescription)
        validated_merged_result = self.__generate_resulting_series(seriesDescription, timeDescription, series_storage_results.data, data_ingestion_results.data if data_ingestion_results != None else None)
        if(validated_merged_result.isComplete):
            return validated_merged_result
        
        # If neither were valid then we log the occurence of missing values and check for interpolation. Then we return the interpolated result.
        log(f'''Request input failure, 
                    Reason: {validated_merged_result.nonCompleteReason} \n 
                    {seriesDescription} \n 
                    {timeDescription} \n 
                    Series Storage Result: {series_storage_results.data} \n 
                    Data Ingestion Result: {None if data_ingestion_results == None else data_ingestion_results.data} \n
                    Merged Result: {validated_merged_result.data}
            ''')
        
        if seriesDescription.maxGapDistance is not None:
            
            interpolated_merged_results = interpolate_series(seriesDescription, timeDescription, validated_merged_result)
            return interpolated_merged_results
        
        return validated_merged_result
      
    
    def interpolate_series(seriesDescription: SeriesDescription, timeDescription: TimeDescription, validated_merged_result: Series) -> Series: 
         """This method will interpolate the results from the query if the gaps between the NaNs are not larger than the maxGapDistance

        Args:
            seriesDescription (SeriesDescription): Describes a set of data
            timeDescription (TimeDescription): Describes the date time properties of the dataset
            validated_merged_result (Series): The merged result of the DB and DI queries

        Returns:
            Series : The merged result of the DB and DI queries
        """
        
         merged_df = pd.DataFrame([input.__dict__ for input in validated_merged_result.data])
         
         merged_df.set_index("timeVerified", inplace=True)
         
         #Fill in the missing predictions and timestamps with NaNs
         filled_merged_df = fill_in_date_gaps(merged_df,timeDescription.fromDateTime, timeDescription.toDateTime, timeDescription.interval,)
         #interpolate with time series interpolation
         largerThanMaxGapDistance, rows_with_nan = check_gap_distance(filled_merged_df, seriesDescription.maxGapDistance, timeDescription.interval)
         if largerThanMaxGapDistance:
             log(f'''Interpolation error,
                    Reason: There are gaps between data that is larger than the maxGapDistance limit.\n 
                    {seriesDescription} \n 
                    {timeDescription} \n 
                    maxGapDistance: {seriesDescription.maxGapDistance}
                    Rows with NaN only: {rows_with_nan}
                ''')
             return validated_merged_result   
             
         filled_merged_df['dataValue'] = filled_merged_df['dataValue'].interpolate(method='linear', limit = seriesDescription.maxGapDistance)
         filled_merged_df.reset_index(inplace=True)
         inputs = [] 
         for __, row in filled_merged_df.iterrows():
             inputs.append(Input(
                 dataValue=row["dataValue"],
                 dataUnit=row["dataUnit"],
                 timeGenerated=row["timeGenerated"],
                 timeVerified=row["timeVerified"],
                 longitude=row["longitude"],
                 latitude=row["latitude"]
             ))  
         validated_merged_result.data = inputs
         validated_merged_result.isComplete = True
         return validated_merged_result
     
    def  check_gap_distance(df: pd.DataFrame, maxGapDistance: timedelta, interval: timedelta):
        """This method will remove all rows with successfull queries leaving just the NaN values. It will then check to make 
        sure that there isn't a distance between two NAN values that is larger than the maxGapDistance. 

        Args:
            df (pd.DataFrame): The dataFrame of data
            maxGapDistance (timedelta): The max gap distance the researcher allows for their model
            interval (timedelta): The time step separating the data points in order

        Returns:
            bool: Determines if a gap is larger than the maxGapDistance
            DataFrame: Dataframe of the rows without NaNs
        """
        # Find any rows that have NaNs
        nan_mask = df.isna().any(axis=1)

        # Extract only rows with NaNs <- Important
        rows_with_nan = df[nan_mask]

        previous_date = None
        nan_gap_size = 1
        for current_date in rows_with_nan.index:

            # If first date, set the previous date to the current date and then continue
            if previous_date is None:

                previous_date = current_date

                continue

            time_difference = current_date - previous_date

            # Since there are only rows with Nans present, if the time difference between the previous date and the current date is 
            # greater than the interval then there is a prediction/data present between the values
            if (time_difference > interval):
                # Reset the counter
                nan_gap_size = 1

            else:
                nan_gap_size += 1

            # If the NaN gap is greater than maxGapDistance, return True
            if(nan_gap_size > maxGapDistance):
                return True

            previous_date = current_date

        # If no NaN gap was greater than maxGapDistance
        return False, rows_with_nan
             
    def fill_in_date_gaps(df : pd.DataFrame , start_date : datetime , end_date : datetime , interval : timedelta ) -> pd.DataFrame:
        """Fills in missing date gaps with NaNs based on given interval.

            Args:
                df [DataFrame]: pandas dataframe

                start_date (datetime): Date to start at

                end_date (datetime): Date to end at

                interval (timedelta): The time step separating the data points in order

            Returns:
                [DataFrame]: pandas dataframe containing desired data
            """

        all_dates = pd.date_range(start=start_date, end=end_date, freq=interval)

        all_dates_df = pd.DataFrame(index=all_dates)

        merged_df = pd.merge(all_dates_df, df, left_index=True, right_index=True, how='left')

        return merged_df
    
    def request_output(self, semaphoreSeriesDescription: SemaphoreSeriesDescription, timeDescription: TimeDescription) -> Series: 
        ''' Takes a description of an output series and attempts to return it
            :param seriesDescription: SemaphoreSeriesDescription -A semaphore series description
            :param timeDescription: TimeDescription - A description about the temporal parts of the output series
            :return series
        '''
        
        ###See if we can get the outputs from the database
        requestedSeries = self.seriesStorage.select_output(semaphoreSeriesDescription, timeDescription)
        return requestedSeries


    def __generate_resulting_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, DBList: list[Input], DIList: list[Input] | None = None) -> Series: 
        """This function validates a set of results against a request. It will generate a list of every expected time stamp and 
        attempt to match inputs to those timestamps. If both a database and data ingestion series are provided it will merge them
        prioritizing results from data ingestion when they conflict with results from the database.
        :param seriesDescription: SemaphoreSeriesDescription - The request description
        :param timeDescription: TimeDescription - The description of the temporal information of the request 
        :param DBList: list[Input] - A list of results to validate from the DB
        :param DIList: list[Input] - A list of results to validate from data ingestion
        :return a validated series: Series

        """
        # If there is a verification Override we handle that first and return before the rest of the logic
        if seriesDescription.verificationOverride != None:

            DBList_valid = len(DBList) == seriesDescription.verificationOverride
            DIList_valid = DIList != None and len(DIList) == seriesDescription.verificationOverride

            valid_list = None
            if DIList_valid: 
                valid_list = DIList
            elif DBList_valid:
                valid_list = DBList

            if valid_list != None:
                result = Series(
                description= seriesDescription, 
                isComplete= True,
                timeDescription= timeDescription,
                )
                result.data = valid_list
                return result
            else:
                result = Series(
                description= seriesDescription, 
                isComplete= False,
                timeDescription= timeDescription,
                nonCompleteReason= 'Failed the verification override check.'
                )
                return result

        # Default logic

        missing_results = 0
        datetimeList = self.__generate_datetime_list(timeDescription) # A list with every time stamp we expect, with a value of none
        
        # Construct a dictionary of the required date times
        datetimeDict =  { dateTime : None for dateTime in datetimeList }

        # Construct a dictionary for the db results
        database_results = { input.timeVerified : input for input in DBList }

        # If there are data ingestion results construct a dictionary for that too
        if DIList != None:
            ingestion_results = { input.timeVerified : input for input in DIList}


        # Iterate over every timestamp we are expecting 
        for datetime in datetimeDict.keys():
            
            # If there are results we prioritize the DI result as thats always freshly acquired
            # If there are no results from either DB or DI, that is missing
            if DIList != None and ingestion_results.get(datetime):
                datetimeDict[datetime] = ingestion_results.get(datetime)
            elif database_results.get(datetime):
                datetimeDict[datetime] = database_results.get(datetime)
            else:
                missing_results = missing_results + 1

        # If there were missing results we assign as such in the series
        # No log here, as this method will be used to also detect if Data Ingestion should be kicked off
        isComplete = True
        reason_string = ''
        if missing_results > 0:
            isComplete = False
            reason_string = f'There were {missing_results} missing results!'

            # If a result was missing, an input was not mapped to that key. Thus its mapped to None. We remove the whole k:v pair as to not pollute the results with None.
            datetimeDict = {k : v for k, v in datetimeDict.items() if v is not None}
            #Taking out the entire pair including the timestamp may make it difficult to interpolate
        result = Series(
            description= seriesDescription, 
            isComplete= isComplete,
            timeDescription= timeDescription,
            nonCompleteReason= reason_string
        )
        result.data = list(datetimeDict.values())
        return result
                
 
    def __generate_datetime_list(self, timeDescription: TimeDescription) -> list:
        """This function creates a list of expected time stamps between a from time and a two time at some interval.
        The keys are the time steps and the values are always set to None.
        If to time and from time are equal, its only a single pair is returned as that describes a single input.
        :param timeDescription: TimeDescription - The description of the temporal information of the request 
        :return list[datetime]
        """
        # If to time == from time this is a request for a single point
        if timeDescription.fromDateTime == timeDescription.toDateTime:
            return [timeDescription.toDateTime]
        
        if timeDescription.interval.total_seconds() == 0:
            return [timeDescription.toDateTime]
        
        # Define the initial time and how many time steps their are.
        initial_time = timeDescription.fromDateTime
        steps = int((timeDescription.toDateTime - timeDescription.fromDateTime) / timeDescription.interval)
        steps = steps + 1 # We increment here as we are inclusive on both sides of the range of datetimes [from time, to time]
        
        # GenerateTimeStamp will calculate a timestamp an amount of steps away from the initial time
        generateTimestamp = lambda initial_time, idx, interval : initial_time + (interval * idx)

        # Perform list comprehension to generate a list of all the time steps we need plus another list of the same size this is all None
        return [generateTimestamp(initial_time, idx, timeDescription.interval) for idx in range(steps)]
        
    