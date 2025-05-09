# -*- coding: utf-8 -*-
#SQLAlchemyORM.py
#-------------------------------
# Created By : Matthew Kastl
# Created Date: 3/26/2023
# version 9.0
#-------------------------------
""" This file is an implementation of the SQLAlchemy ORM geared towards Semaphore and its schema. 
 """ 
#-------------------------------
# 
#
#Imports
from sqlalchemy import create_engine as sqlalchemy_create_engine
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, UniqueConstraint, Engine, ForeignKey, CursorResult, Select, select, distinct, Boolean, Interval, text
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import insert
from os import getenv
from datetime import timedelta, datetime
import pandas as pd
from pandas import DataFrame

from SeriesStorage.ISeriesStorage import ISeriesStorage

from DataClasses import Series, SeriesDescription, SemaphoreSeriesDescription, TimeDescription, get_input_dataFrame, get_output_dataFrame
from utility import log


class SQLAlchemyORM_Postgres(ISeriesStorage):
 
    def __init__(self) -> None:
        """Constructor generates an a db schema. Automatically creates the 
        metadata object holding the defined schema.
        """
        self.__create_engine(getenv('DB_LOCATION_STRING'), False)
        self.__metadata = MetaData()
        self.__metadata.reflect(bind=self.__get_engine())

    #############################################################################################
    ################################################################################## Public methods
    #############################################################################################

    def select_input(self, seriesDescription: SeriesDescription, timeDescription : TimeDescription) -> Series:
        """Selects a given series given a SeriesDescription and TimeDescription
           :param seriesDescription: SeriesDescription - A series description object
           :param timeDescription: TimeDescription - A hydrated time description object
        """
        inputTable = self.__metadata.tables['inputs']
        statement = (select(inputTable)
            .where(inputTable.c.dataSource == seriesDescription.dataSource)
            .where(inputTable.c.dataLocation == seriesDescription.dataLocation)
            .where(inputTable.c.dataSeries == seriesDescription.dataSeries)
            .where(inputTable.c.dataDatum == seriesDescription.dataDatum)
            .where(inputTable.c.verifiedTime >= timeDescription.fromDateTime)
            .where(inputTable.c.verifiedTime <= timeDescription.toDateTime)
            )
        tupleishResult = self.__dbSelection(statement).fetchall()
        df_inputResult = self.__splice_input(tupleishResult)
        
        # Prune the results removing any data that does not align with the interval that was requested
        df_prunedInputs = df_inputResult.copy(deep=True)
        if timeDescription.interval != None and timeDescription.interval.total_seconds() != 0:
            for i in range(len(df_inputResult)):
                if not (df_inputResult.iloc[i]['timeVerified'].timestamp() % timeDescription.interval.total_seconds() == 0):
                    df_prunedInputs.drop(i, inplace=True)

        series = Series(seriesDescription, True, timeDescription)
        series.dataFrame = df_prunedInputs
        return series
    
    def select_specific_output(self, semaphoreSeriesDescription: SemaphoreSeriesDescription, timeDescription : TimeDescription) -> Series:
        """Selects an output series given a SemaphoreSeriesDescription and TimeDescription
           :param semaphoreSeriesDescription: SemaphoreSeriesDescription - A  semaphore series description object
           :param timeDescription: TimeDescription - A hydrated time description object
        """

        series = Series(semaphoreSeriesDescription, True, timeDescription)
        

        outputTable = self.__metadata.tables['outputs']
        #Get the lead time for time calculations
        statement = (select(distinct(outputTable.c.leadTime))
                    .where(outputTable.c.dataLocation == semaphoreSeriesDescription.dataLocation)
                    .where(outputTable.c.dataSeries == semaphoreSeriesDescription.dataSeries)
                    .where(outputTable.c.dataDatum == semaphoreSeriesDescription.dataDatum)
                    .where(outputTable.c.modelName == semaphoreSeriesDescription.modelName)
                    )
        leadTimes = self.__dbSelection(statement).fetchall()
        if len(self.__dbSelection(statement).fetchall()) == 0: #If no lead time is found for some reason return nothing and log this
            log(f'SQLAlchemyORM | select_output | No leadtime found for SemaphoreSeriesDescription:{semaphoreSeriesDescription}')
            return series
            
        leadTime = leadTimes[0]
        fromGeneratedTime = timeDescription.fromDateTime - leadTime[0]
        toGeneratedTime = timeDescription.toDateTime - leadTime[0]
         
        statement = (select(outputTable)
                    .where(outputTable.c.modelName == semaphoreSeriesDescription.modelName)
                    .where(outputTable.c.dataLocation == semaphoreSeriesDescription.dataLocation)
                    .where(outputTable.c.dataSeries == semaphoreSeriesDescription.dataSeries)
                    .where(outputTable.c.dataDatum == semaphoreSeriesDescription.dataDatum)
                    .where(outputTable.c.timeGenerated >= fromGeneratedTime)
                    .where(outputTable.c.timeGenerated <= toGeneratedTime)
                    )
        tupleishResult = self.__dbSelection(statement).fetchall()
        series.dataFrame = self.__splice_output(tupleishResult)
        return series
    

    def select_output(self, model_name: str, from_time: datetime, to_time: datetime) -> Series | None: 
        ''' This selects outputs based just on a model name and a time range, all other information is inferred
            :param model_name: str - The name of the model to query
            :param to_time: datetime - The latest time to include
            :param from_time: datetime - The earliest time to include
        '''        

        # Get the lead time for time calculations
        # Because we are inferring model information
        # We select the latest version of the model under that name
        # and the lead time from its latest prediction
        outputTable = self.__metadata.tables['outputs']
        statement = (select(outputTable.c.leadTime)
                    .where(outputTable.c.modelName == model_name)
                    .order_by(outputTable.c.modelVersion.desc())
                    .order_by(outputTable.c.timeGenerated.desc())
                    )
        leadTimes = self.__dbSelection(statement).fetchall()
        if len(self.__dbSelection(statement).fetchall()) == 0: #If no lead time is found for some reason return nothing and log this
            log(f'SQLAlchemyORM | select_latest_output | No leadtime found for model_name:{model_name}')
            return None
            
        leadTime = leadTimes[0]
        fromGeneratedTime = from_time - leadTime[0]
        toGeneratedTime = to_time - leadTime[0]
         
        statement = (select(outputTable)
                    .where(outputTable.c.modelName == model_name)
                    .where(outputTable.c.timeGenerated >= fromGeneratedTime)
                    .where(outputTable.c.timeGenerated <= toGeneratedTime)
                    )
        tupleishResult = self.__dbSelection(statement).fetchall()

        if not tupleishResult: # If there are no results, no model information can be inferred 
            return None    

        outputResult = self.__splice_output(tupleishResult)

        # Parse out model information from first output result
        description = SemaphoreSeriesDescription(tupleishResult[0][3], tupleishResult[0][4], tupleishResult[0][8], tupleishResult[0][7], tupleishResult[0][6])
        series = Series(description, False)
        series.dataFrame = outputResult
        return series
    

    def select_latest_output(self, model_name: str) -> Series | None: 
        ''' This selects *all* outputs based just on a model name and a time range, all other information is inferred
            :param model_name: str - The name of the model to query

            NOTE:: Things like model version and time will just be the latest in the DB
        '''        
        outputTable = self.__metadata.tables['outputs']
        statement = (select(outputTable)
                    .where(outputTable.c.modelName == model_name)
                    .order_by(outputTable.c.modelVersion.desc())
                    .order_by(outputTable.c.timeGenerated.desc())
                    )
        tupleishResult = self.__dbSelection(statement).first() # because of order this should be latest

        if not tupleishResult: # If there are no results, no model information can't be inferred 
            return None    

        latest_time = tupleishResult[1]

        # STEP 2: Get ALL outputs with the same timeGenerated
        statement = (
            select(outputTable)
            .where(outputTable.c.modelName == model_name)
            .where(outputTable.c.timeGenerated == latest_time)
        )

        result = self.__dbSelection(statement).fetchall()

        if not result:
            return None
        
        outputResult = self.__splice_output(result)  # Use all fetched outputs, not just the first one 

        # Parse out model information from first output result
        first = result[0]
        description = SemaphoreSeriesDescription(first[3], first[4], first[8], first[7], first[6])
        series = Series(description, False)
        series.dataFrame = outputResult
        return series
    
    def find_external_location_code(self, sourceCode: str, location: str, priorityOrder: int = 0) -> str:
        """Returns a data source location code based off of passed parameters
           :param sourceCode: str - the data source code (noaaT&C)
           :param location: str - the local location name 
           :param priorityOrder: int - priority of which locations to go to if one is unavailable 
        """
        dataLocation_dataSource_mapping = self.__metadata.tables['dataLocation_dataSource_mapping']
        statement = (select(dataLocation_dataSource_mapping.c.dataSourceLocationCode)
                     .where(dataLocation_dataSource_mapping.c.dataSourceCode == sourceCode)
                     .where(dataLocation_dataSource_mapping.c.dataLocationCode == location)
                     .where(dataLocation_dataSource_mapping.c.priorityOrder == priorityOrder)
                    )
        dataSourceLocationCode = self.__dbSelection(statement).fetchall()[0]
        return dataSourceLocationCode[0]

    def find_lat_lon_coordinates(self, locationCode: str) -> tuple:
        """Returns lat and lon tuple
           :param sourceCode: str - the data source code (noaaT&C)
           :param location: str - the local location name 
           :param priorityOrder: int - priority of which locations to go to if one is unavailable 
        """
        ref_dataLocationTable = self.__metadata.tables['ref_dataLocation']
        statement = (select(ref_dataLocationTable.c.latitude, ref_dataLocationTable.c.longitude)
                     .where(ref_dataLocationTable.c.code == locationCode)
                    )
        latLon = self.__dbSelection(statement).first()
        return (latLon[0], latLon[1])
        

    def insert_input(self, series: Series) -> Series:
        """This method inserts actual/predictions into the input table
            :param series: Series - A series object with a time description, series description, and input data
            :return Series - A series object that contains the actually inserted  data
        """

        if(type(series.description).__name__ != 'SeriesDescription'): raise ValueError('Description should be type SeriesDescription')

        # If dataValue is a list its an ensemble
        isEnsemble = isinstance(series.dataFrame['dataValue'].iloc[0], list) 
        now = datetime.now()
        insertionRows = []

        for df_index, row in series.dataFrame.iterrows():
            # We need to iterate over every value in dataValue if it is an ensemble
            # but non ensemble inputs are just a single value so we convert them
            # temporarily to a list to make the iteration easier
            dataValues = row["dataValue"] if isEnsemble else [ row["dataValue"] ] 
            for value_index, value in enumerate(dataValues):
                new_row = {
                    "generatedTime": series.dataFrame.iloc[df_index]['timeGenerated'], 
                    "acquiredTime": now, 
                    "verifiedTime": series.dataFrame.iloc[df_index]['timeVerified'], 
                    "dataValue": value, 
                    "isActual": False if series.description.dataSeries[0] == 'p' else True, 
                    "dataUnit": series.dataFrame.iloc[df_index]['dataUnit'], 
                    "dataSource": series.description.dataSource, 
                    "dataLocation": series.description.dataLocation,
                    "dataSeries": series.description.dataSeries, 
                    "dataDatum": series.description.dataDatum, 
                    "latitude": series.dataFrame.iloc[df_index]['latitude'], 
                    "longitude": series.dataFrame.iloc[df_index]['longitude'],
                    "ensembleMemberID": value_index if isEnsemble else None # Only set for ensemble inputs
                }
                insertionRows.append(new_row)         
        
        # Insert the rows into the inputs table returning what is inserted as a sanity check
        with self.__get_engine().connect() as conn:
            cursor = conn.execute(insert(self.__metadata.tables['inputs'])
                                    .on_conflict_do_nothing('inputs_AK00')
                                    .returning(self.__metadata.tables['inputs'])
                                    .values(insertionRows)
                                )
            result = cursor.fetchall()
            conn.commit()
        
        # Create a series object to return with the inserted data
        resultSeries = Series(series.description, True, series.timeDescription)
        resultSeries.dataFrame = self.__splice_input(result) #Turn tuple objects into actual objects
        return resultSeries
    
    def insert_output_and_model_run(self, output_series: Series, execution_time: datetime, return_code: int) -> tuple[Series, dict | None]:
        """This method inserts actual/predictions into the output table and model run information into the model run table
            :param series: Series - A series object with a time description,  semaphore series description, and outputdata
            :param dateTime: execution_time - A datetime object set to the time this instance of semaphore was ran
            :param int: return_code - A int code for the run
            :return tuple[Series, dict]: 
                - A series object that contains the actually inserted data
                - The results from inserting into the model_runs table
        """

        output_series, output_ids = self.insert_output(output_series)


        if len(output_ids) == 0:
            log('WARNING:: Series Storage was told to insert to both the output and model run table. This failed because no outputs were inserted. This could be caused by the prediction already existing in the database!')
            return output_series, None

        model_run_rows = []
        for id in output_ids:
            model_run_row = {"outputID": None, "executionTime": None, "returnCode": None}
            model_run_row["outputID"] = id
            model_run_row["executionTime"] = execution_time
            model_run_row["returnCode"] = return_code
            model_run_rows.append(model_run_row)

        model_runs = self.__metadata.tables['model_runs']
        with self.__get_engine().connect() as conn:
            cursor = conn.execute(insert(model_runs)
                                  .returning(model_runs)
                                  .values(model_run_rows)
                                  )
            model_run_result = cursor.fetchall()
            conn.commit()


        return output_series, model_run_result
        

    def insert_output(self, series: Series) -> tuple[Series, list[int]]:
        """This method inserts actual/predictions into the output table
            :param series: Series - A series object with a time description,  semaphore series description, and outputdata
            :return tuple[Series, list[int]]: 
                - A series object that contains the actually inserted data
                - A list of the pK ids the rows took
        """

        if(type(series.description).__name__ != 'SemaphoreSeriesDescription'): raise ValueError('Description should be type SemaphoreSeriesDescription')

        # If dataValue is a list its an ensemble
        isEnsemble = isinstance(series.dataFrame['dataValue'].iloc[0], list) 

        insertionRows = []
        for df_index, row in series.dataFrame.iterrows():

            # We need to iterate over every value in dataValue if it is an ensemble
            # but non ensemble inputs are just a single value so we convert them
            # temporarily to a list to make the iteration easier
            dataValues = row["dataValue"] if isEnsemble else [ row["dataValue"] ] 

            for value_index, value in enumerate(dataValues):
                insertionValueRow = {
                    "timeGenerated": series.dataFrame.iloc[df_index]['timeGenerated'], 
                    "leadTime": series.dataFrame.iloc[df_index]['leadTime'], 
                    "modelName": series.description.modelName, 
                    "modelVersion": series.description.modelVersion, 
                    "dataValue": value, 
                    "dataUnit": series.dataFrame.iloc[df_index]['dataUnit'], 
                    "dataLocation": series.description.dataLocation,
                    "dataSeries": series.description.dataSeries, 
                    "dataDatum": series.description.dataDatum, 
                    "ensembleMemberID": value_index if isEnsemble else None # Only set for ensemble inputs
                }
                insertionRows.append(insertionValueRow)  

        # Insert the rows into the outputs table returning what is inserted as a sanity check
        outputTable = self.__metadata.tables['outputs']
        with self.__get_engine().connect() as conn:
            cursor = conn.execute(insert(outputTable)
                                  .on_conflict_do_nothing('outputs_AK00')
                                  .returning(outputTable)
                                  .values(insertionRows)
                                  )
            result = cursor.fetchall()
            conn.commit()

        # Create a series object to return with the inserted data
        resultSeries = Series(series.description, True, series.timeDescription)
        resultSeries.dataFrame = self.__splice_output(result) #Turn tuple objects into actual objects
        ids = [row[0] for row in result]
        return resultSeries, ids

    #############################################################################################
    ################################################################################## DB Managment Methods
    #############################################################################################

    def __create_engine(self, parmaString: str, echo: bool ) -> None: #"sqlite+pysqlite:///:memory:"
        """Creates an engine object and tethers it to this interface class as an atribute

        Parameters:
            permaString: str - An sqlalchemy string that defines the location the engine should point to: (e.g. "sqlite+pysqlite:///:memory:")
            echo: str - Weather or not the engine should echo to stdout
        """
        self._engine = sqlalchemy_create_engine(parmaString, echo=echo) #, pool_pre_ping=True

    
    def __get_engine(self) -> Engine:
        """Fetches the engine attribute. Requires the engine attribute to be created.
        See: DBManager.create_engine()
        """

        if not hasattr(self, '_engine') or self._engine == None:
            raise Exception("An engine was requested from DBManager, but no engine has been created. See DBManager.create_engine()")
        else:
            return self._engine

    #############################################################################################
    ################################################################################## DB Interaction private methods
    #############################################################################################

    def __dbSelection(self, stmt: Select) -> CursorResult:
        """Runs a selection statement 
        Parameters:
            stmt: SQLAlchemy Select - The statement to run
        Returns:
            SQLAlchemy CursorResult
        """

        with self.__get_engine().connect() as conn:
            result = conn.execute(stmt)

        return result

    def __splice_input(self, results: list[tuple]) -> DataFrame:
        """ Converts DB rows to a proper input dataframe to be packed into a series.
        This method also handles ensemble data by grouping them into single rows as expected by Semaphore.
        :param list[tupleish] -a list of selections from the table formatted in tupleish
        :return: DataFrame - a dataframe with the data formatted for use in a series
        """

        # Convert returned DB rows into a dataframe to make manipulation easier 
        df_results = pd.DataFrame(
            data=results, 
            columns=[   
                "id", "generatedTime", "acquiredTime", "verifiedTime", "dataValue", 
                "isActual", "dataUnit", "dataSource", "dataLocation", "dataSeries", 
                "dataDatum", "latitude", "longitude", "ensembleMemberID"
            ]
        )

        # A formatted dataframe to place the spliced data 
        df_out = get_input_dataFrame()

        # We have to post process the results as this could be ensemble data or not
        # grouping by verifiedTime groups the data by time point
        for _, group in df_results.groupby("verifiedTime"):

            # ensembleMemberID is None if not an ensemble, so we check the first row of the group
            firstRow = group.iloc[0]
            isEnsemble = firstRow["ensembleMemberID"] is not None
            
            # If its an ensemble the dataValue is all the values sorted by there ID (to ensure correct order)
            # If not an ensemble we just take the solitary dataValue
            if isEnsemble:
                group = group.sort_values(by=("ensembleMemberID"))
                dataValues = group["dataValue"].to_list()
            else:
                dataValues = group["dataValue"].iloc[0]
            
            # Pack the data into the out dataframe
            df_out.loc[len(df_out)] = [
                dataValues,                   # dataValue
                firstRow["dataUnit"],         # dataUnit
                firstRow["verifiedTime"],     # timeVerified
                firstRow["generatedTime"],    # timeGenerated
                firstRow["longitude"],        # longitude
                firstRow["latitude"]          # latitude
            ]

        return df_out


    def __splice_output(self, results: list[tuple]) -> DataFrame:
        """Converts DB row results into a proper output dataframe to be packed into a series.
        param: list[tupleish] - a list of selections from the table formatted in tupleish
        returns: DataFrame - a dataframe with the data formatted for use in a series
        """

        # Convert returned DB rows into a dataframe to make manipulation easier 
        df_results = pd.DataFrame(
            data=results, 
            columns=[
                "ID", "timeGenerated", "leadTime", "modelName", "modelVersion", "dataValue", 
                "dataUnit", "dataLocation", "dataSeries", "dataDatum", "ensembleMemberID"
            ]
        )
        
        # A formatted dataframe to place the spliced data 
        df_out = get_output_dataFrame()

        # We have to post process the results as this could be ensemble data or not
        # we group data with identical temporal information    
        for _, group in df_results.groupby(["timeGenerated", "leadTime"]):
    
            firstRow = group.iloc[0]
            isEnsemble = firstRow["ensembleMemberID"] is not None
            
            # If its an ensemble the dataValue is all the values sorted by there ID (to ensure correct order)
            # If not an ensemble we just take the solitary dataValue
            if isEnsemble:
                group = group.sort_values(by=("modelName"))
                dataValues = group["dataValue"].to_list()
            else:
                dataValues = group["dataValue"].iloc[0]
            
            # Pack the data into the out dataframe
            df_out.loc[len(df_out)] = [
                dataValues,                 # dataValue    
                firstRow["dataUnit"],       # dataUnit
                firstRow["timeGenerated"],  # timeGenerated
                firstRow["leadTime"]        # leadTime
            ]

        return df_out


    def insert_lat_lon_test(self, code: str, displayName: str, notes: str, latitude: str, longitude: str):
        """This method inserts lat and lon information
        """
        #Construct DB row to insert
        insertionValueRow = {"code": code, "displayName": displayName, "notes": notes, "latitude": latitude, "longitude": longitude}
        
        with self.__get_engine().connect() as conn:
            conn.execute(insert(self.__metadata.tables['ref_dataLocation'])
                        .values(insertionValueRow))
            conn.commit()

    def insert_external_location_code(self, dataLocationCode: str, dataSourceCode: str, dataSourceLocationCode: str, priorityOrder: int):
        """This method inserts external location code information
        """
        #Construct DB row to insert
        insertionValueRow = {"dataLocationCode": dataLocationCode, "dataSourceCode": dataSourceCode, "dataSourceLocationCode": dataSourceLocationCode, "priorityOrder": priorityOrder}
        
        with self.__get_engine().connect() as conn:
            conn.execute(insert(self.__metadata.tables['dataLocation_dataSource_mapping'])
                        .values(insertionValueRow))
            conn.commit()