# -*- coding: utf-8 -*-
#SQLAlchemyORM_Postgres.py
#-------------------------------
# Created By : Matthew Kastl
# Created Date: 3/26/2023
# version 10.0
#-------------------------------
"""
This file is an implementation of the SQLAlchemy ORM geared towards Semaphore and its schema.

NOTE:: As of version 10.0, this storage class uses binary serialization for 
ndarrays in the dataValue column of dataframes.
Every model run will have their ndarray serialized to bytes before insertion into the database 
and deserialized back to ndarrays after selection from the database.
""" 
#-------------------------------
# 
#
#Imports
from itertools import groupby
from sqlalchemy import create_engine as sqlalchemy_create_engine
from sqlalchemy import String, MetaData, Engine, CursorResult, Select, select, distinct, text, bindparam
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects import postgresql
from os import getenv
from datetime import timedelta, datetime, timezone
import pandas as pd
from pandas import DataFrame
import numpy as np
from io import BytesIO
from numpy import ndarray

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
        """Selects a given series given a SeriesDescription and TimeDescription using splice_input to give the latest generated time per verified time.
        
           Data Assumptions (about the inputs table): 
            - Each ensemble input is logically complete: for a given run, every ensemble member 
             has values for every verifiedTime in the requested window. TWC always generates 
             the full ensemble group with all generatedTimes being the same for each member.
            - For a given dataSource, dataLocation, dataSeries, dataDatum, verifiedTime,
            ensembleMemberID, at least one row has a given generatedTime, so "latest generatedTime" is well-defined.
            - The final output may come from multiple model runs, such as different generatedTimes across verifiedTimes. 
            Each verified time will have an ensemble group with the same generated time, but the generated 
            time's will likely be different per verified time.
           
           Query Summary: The data is ordered into groups with the same verifiedTime AND ensembleMemberId. 
           From EACH group the latest generatedTime is selected. This query effectively gathers the latest 
           generated time for each ensemble and non ensemble members.
           
           :param seriesDescription: SeriesDescription - A series description object
           :param timeDescription: TimeDescription - A hydrated time description object
        """
        inputs_select_latest_stmt = text(f""" 
        SELECT DISTINCT ON ("verifiedTime", "ensembleMemberID")
            i."id",
            i."generatedTime",
            i."acquiredTime",
            i."verifiedTime",
            i."dataValue",
            i."isActual",
            i."dataUnit",
            i."dataSource",
            i."dataLocation",
            i."dataSeries",
            i."dataDatum",
            i."latitude",
            i."longitude",
            i."ensembleMemberID"
            FROM inputs AS i
            WHERE         
                i."dataSource"   = :dataSource
                AND i."dataLocation" = :dataLocation
                AND i."dataSeries"   = :dataSeries
                AND {'i."dataDatum" = :dataDatum' if seriesDescription.dataDatum is not None else 'i."dataDatum" IS NULL'}
                AND i."verifiedTime" BETWEEN :from_dt AND :to_dt
            ORDER BY
            i."verifiedTime",
            i."ensembleMemberID",
            i."generatedTime" DESC
        """)
        
        # Only bind dataDatum if it's not None
        bind_params = {
            'dataSource': seriesDescription.dataSource,
            'dataLocation': seriesDescription.dataLocation,
            'dataSeries': seriesDescription.dataSeries,
            'from_dt': timeDescription.fromDateTime,
            'to_dt': timeDescription.toDateTime 
        }
        if seriesDescription.dataDatum is not None:
            bind_params['dataDatum'] = seriesDescription.dataDatum
        
        inputs_select_latest_stmt = inputs_select_latest_stmt.bindparams(**bind_params)

        tupleishResult = self.__dbSelection(inputs_select_latest_stmt).fetchall()
        
        df_inputResult = self.__splice_input(tupleishResult)
        
        
        # Sending all rows found downstream the input gatherer will handle interval alignment
        series = Series(seriesDescription, timeDescription)
        series.dataFrame = df_inputResult
        return series
    
    def select_specific_output(self, semaphoreSeriesDescription: SemaphoreSeriesDescription, timeDescription : TimeDescription) -> Series:
        """
        Selects an output series given a SemaphoreSeriesDescription and TimeDescription.
        This method first queries for the lead time for a model run that matches the model and data description
        then it uses that lead time to adjust the from and to generated times in the time description to
        make a second query for outputs that match a more specific description. This works because
        each model corresponds to a specific lead time, so all rows for a given model name will have the same lead time.

        This query can return many rows at once if there are many generated times that fall within the time window after lead time adjustment.

        :param semaphoreSeriesDescription: SemaphoreSeriesDescription - A  semaphore series description object
        :param timeDescription: TimeDescription - A hydrated time description object
        """

        series = Series(semaphoreSeriesDescription, timeDescription)
        
        stmt = text(f"""
            SELECT "leadTime" FROM outputs
            WHERE "dataLocation" = :dataLocation
            AND "dataSeries" = :dataSeries
            AND {'"dataDatum" = :dataDatum' if semaphoreSeriesDescription.dataDatum is not None else '"dataDatum" IS NULL'}
            AND "modelName" = :modelName
            AND "modelVersion" = :modelVersion
            LIMIT 1
        """)

        bind_params = {
            'dataLocation': semaphoreSeriesDescription.dataLocation,
            'dataSeries': semaphoreSeriesDescription.dataSeries,
            'modelName': semaphoreSeriesDescription.modelName,
            'modelVersion': semaphoreSeriesDescription.modelVersion
        }

        if semaphoreSeriesDescription.dataDatum is not None:
            bind_params['dataDatum'] = semaphoreSeriesDescription.dataDatum

        stmt = stmt.bindparams(**bind_params)
        leadTime = self.__dbSelection(stmt).fetchone()

        # if no lead time is found for some reason return nothing and log this
        if leadTime is None:
            log(f'SQLAlchemyORM | select_specific_output | No leadtime found for SemaphoreSeriesDescription:{semaphoreSeriesDescription}')
            return series
        
        fromGeneratedTime = timeDescription.fromDateTime - leadTime[0]
        toGeneratedTime = timeDescription.toDateTime - leadTime[0]

        stmt = text(f"""
            SELECT * FROM outputs
            WHERE "modelName" = :modelName
            AND "modelVersion" = :modelVersion
            AND "dataLocation" = :dataLocation
            AND "dataSeries" = :dataSeries
            AND {'"dataDatum" = :dataDatum' if semaphoreSeriesDescription.dataDatum is not None else '"dataDatum" IS NULL'}
            AND "timeGenerated" >= :fromGeneratedTime
            AND "timeGenerated" <= :toGeneratedTime
        """)

        bind_params = {
            'modelName': semaphoreSeriesDescription.modelName,
            'modelVersion': semaphoreSeriesDescription.modelVersion,
            'dataLocation': semaphoreSeriesDescription.dataLocation,
            'dataSeries': semaphoreSeriesDescription.dataSeries,
            'fromGeneratedTime': fromGeneratedTime,
            'toGeneratedTime': toGeneratedTime
        }

        if semaphoreSeriesDescription.dataDatum is not None:
            bind_params['dataDatum'] = semaphoreSeriesDescription.dataDatum

        stmt = stmt.bindparams(**bind_params)
        tupleishResult = self.__dbSelection(stmt).fetchall()
        series.dataFrame = self.__splice_output(tupleishResult)
        return series
    

    def select_output(self, model_name: str, from_time: datetime, to_time: datetime) -> Series | None: 
        ''' This selects outputs based just on a model name and a time range, all other information is inferred.
            The modelname will be used to select the lead time for the most recent model run with that model name,
            then a second query is made to select all rows for that model name that have a generated time that falls within
            the calculated time range. This works beacuse each model corresponds to a specific lead time,
            so all rows for a given model name will have the same lead time.
            
            Many rows will be returned if there are many rows for that time range.

            :param model_name: str - The name of the model to query
            :param to_time: datetime - The latest time to include
            :param from_time: datetime - The earliest time to include
        '''

        # Get the lead time for time calculations
        # Because we are inferring model information
        # We select the latest version of the model under that name
        # and the lead time from its latest prediction
        stmt = text("""
            SELECT "leadTime" FROM outputs
            WHERE "modelName" = :model_name
            ORDER BY "modelVersion" DESC, "timeGenerated" DESC
            LIMIT 1
        """)

        bind_params = {
            'model_name': model_name
        }

        stmt = stmt.bindparams(**bind_params)
        leadTime = self.__dbSelection(stmt).fetchone()
        
        # if no lead time is found for some reason return nothing and log this
        if leadTime is None: 
            log(f'SQLAlchemyORM | select_output | No leadtime found for model_name:{model_name}')
            return None
        
        fromGeneratedTime = from_time - leadTime[0]
        toGeneratedTime = to_time - leadTime[0]
        
        stmt = text("""
            SELECT
            "id",
            "timeGenerated",
            "leadTime",
            "modelName",
            "modelVersion",
            "dataValue",
            "dataUnit",
            "dataLocation",
            "dataSeries",
            "dataDatum"
            FROM outputs
            WHERE "modelName" = :model_name
            AND "timeGenerated" >= :fromGeneratedTime
            AND "timeGenerated" <= :toGeneratedTime
        """)

        bind_params = {
            'model_name': model_name,
            'fromGeneratedTime': fromGeneratedTime,
            'toGeneratedTime': toGeneratedTime
        }

        stmt = stmt.bindparams(**bind_params)
        tupleishResult = self.__dbSelection(stmt).fetchall()

        if not tupleishResult:
            return None    

        outputResult = self.__splice_output(tupleishResult)

        # Parse out model information from first output result
        # this is constant metadata across all the rows returned
        row = tupleishResult[0]
        description = SemaphoreSeriesDescription(
            row[3],   # modelName
            row[4],   # modelVersion
            row[8],   # dataSeries
            row[7],   # dataLocation
            row[9]    # dataDatum
        )
        series = Series(description)
        series.dataFrame = outputResult
        return series
    

    def select_latest_output(self, model_names: list[str]) -> list[Series] | None: 
        ''' 
        This selects the latest output for each model in the list of model names, all other information is inferred.
        NOTE:: This will return the latest prediction, per model, that was generated regardless of version.
        '''   

        stmt_collect_all_latest_outputs = text("""
        WITH latest_time_per_model AS (
            SELECT 
                o."modelName",
                MAX(o."timeGenerated") AS latest_time
            FROM outputs AS o
            WHERE o."modelName" IN :model_names
            GROUP BY o."modelName"
        )
        SELECT 
            o."id",
            o."timeGenerated",
            o."leadTime",
            o."modelName",
            o."modelVersion",
            o."dataValue",
            o."dataUnit",
            o."dataLocation",
            o."dataSeries",
            o."dataDatum"
        FROM outputs AS o
        INNER JOIN latest_time_per_model AS ltpm
            ON o."modelName" = ltpm."modelName"
            AND o."timeGenerated" = ltpm.latest_time
        ORDER BY
            o."modelName"
        """)     
        stmt_collect_all_latest_outputs = stmt_collect_all_latest_outputs.bindparams(bindparam("model_names", value=tuple(model_names), expanding=True, type_=String))
        result = self.__dbSelection(stmt_collect_all_latest_outputs).fetchall()
        print(result)
        
        if not result:
            return None
        
        # Each model will be processed into a Series individually 
        results = []
        for row in result:
            # wrap the single row in a list for splice output
            print(row)
            df_parsed_data = self.__splice_output([row]) 

            # create the series object for this model
            series = Series(SemaphoreSeriesDescription(
                row[3],      # modelName
                row[4],      # modelVersion
                row[8],      # dataSeries
                row[7],      # dataLocation
                row[9]),     # dataDatum
                None         # time description isn't needed since this will be the most recent prediction for the model
            )
            series.dataFrame = df_parsed_data
            results.append(series)
        return results
    
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
        now = datetime.now(timezone.utc)
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

    
        # It seems that Postgres has a limit of 65535 parameters per query. If you have a lot of rows (say 6700, then you only have ~9 parameters per row)
        # Our rows have more parameters than that so we if we have more than 999 rows we are trying to insert we need to batch them
        batch_size = 999  # Set your desired batch size
        batched_rows = [insertionRows[i:i + batch_size] for i in range(0, len(insertionRows), batch_size)]
        
        for batch in batched_rows:
            # Insert the rows into the inputs table returning what is inserted as a sanity check
            # On conflict we update the acquired time to now
            with self.__get_engine().connect() as conn:
                cursor = conn.execute(insert(self.__metadata.tables['inputs'])
                                        .values(batch)
                                        .on_conflict_do_update(constraint='inputs_AK00', set_={"acquiredTime": now})
                                        .returning(self.__metadata.tables['inputs'])
                                    )
                result = cursor.fetchall()
                conn.commit()
        
        # Create a series object to return with the inserted data
        resultSeries = Series(series.description, series.timeDescription)
        resultSeries.dataFrame = self.__splice_input(result) #Turn tuple objects into actual objects
        return resultSeries
    
    def insert_output_and_model_run(self, output_series: Series, execution_time: datetime, return_code: int) -> tuple[Series, tuple | None]:
        """
        This method inserts actual/predictions into the output table and model run information into the model run table.
        The output data for a model is a dataframe with 1 row per model run where the dataValue column of this dataframe contains
        an ndarray with the expected shape for a given model. The shape of the ndarray is 3D with dimensions corresponding
        to (members, inputs, outputs).
            
        :param output_series: Series - A series object with a time description,  semaphore series description, and outputdata
        :param dateTime: execution_time - A datetime object set to the time this instance of semaphore was ran
        :param int: return_code - A int code for the run
        
        :returns tuple[Series, tuple | None]: 
            Series - A series object with a semaphore series description, time description, and dataframe
                where the dataframe has the data that was actually inserted into the outputs table.

            tuple | None - A tuple representing the row inserted into the model run table or None if no row
                was inserted due to a conflict with an existing row.
                The tuple will have the order of (id, outputID, executionTime, returnCode)
                where the id is the PK for the model run table and outputID is a FK to the row inserted into the outputs table for this model run.
        """

        output_series, saved_output_id = self.insert_output(output_series)

        if saved_output_id is None:
            log('WARNING:: Series Storage was told to insert to both the output and model run table. This failed because no outputs were inserted. This could be caused by the prediction already existing in the database!')
            return output_series, None

        model_run_row = {
            "outputID": saved_output_id,
            "executionTime": execution_time,
            "returnCode": return_code
        }

        stmt = text("""
        INSERT INTO model_runs (
            "outputID",
            "executionTime",
            "returnCode"
        )
        VALUES (
            :outputID,
            :executionTime,
            :returnCode
        )
        RETURNING *;
        """)

        bind_params = {
            'outputID': model_run_row["outputID"],
            'executionTime': model_run_row["executionTime"],
            'returnCode': model_run_row["returnCode"]
        }
        stmt = stmt.bindparams(**bind_params)

        with self.__get_engine().connect() as conn:
            cursor = conn.execute(stmt)
            model_run_result = cursor.fetchone()
            conn.commit()

        """
        TODO:: Change what is returned to a modelRun object instead of
        returning a database result directly for model_run_result.
        """

        return output_series, model_run_result
        

    def insert_output(self, series: Series) -> tuple[Series, int | None]:
        """
        This method inserts a single row for actual/predictions into the output table

        :param series: Series - A series object with a time description, semaphore series description, and a
            dataframe with exactly 1 row to insert. This row should have an ndarray in its dataValue column that
            should be of the expected shape for this model run with dimensions corresponding to (members, inputs, outputs).
        
        :returns tuple[Series, int | None]: 
            - A series object that contains the dataframe that holds the data that was actually inserted
            - An integer representing the primary key ID the output took in the database
                or None if no row was inserted due to a conflict with an existing row
        """

        if(type(series.description).__name__ != 'SemaphoreSeriesDescription'): raise ValueError('Description should be type SemaphoreSeriesDescription')

        if len(series.dataFrame) != 1: raise ValueError(f'Output series dataframe should only have one row, got {len(series.dataFrame)}')
    
        # pack the output data into a row
        output_row = series.dataFrame.iloc[0]
        serialized_data_results = self.__serialize_data(output_row['dataValue'])# serialize the ndarray to bytes
        row_to_insert = {
            "timeGenerated": output_row['timeGenerated'],
            "leadTime": output_row['leadTime'],
            "modelName": series.description.modelName,
            "modelVersion": series.description.modelVersion,
            "dataValue": serialized_data_results,    
            "dataUnit": output_row['dataUnit'],
            "dataLocation": series.description.dataLocation,
            "dataSeries": series.description.dataSeries,
            "dataDatum": series.description.dataDatum,
        }

        stmt = text("""
        INSERT INTO outputs (
            "timeGenerated",
            "leadTime",
            "modelName",
            "modelVersion",
            "dataValue",
            "dataUnit",
            "dataLocation",
            "dataSeries",
            "dataDatum"
            )
        VALUES (
            :timeGenerated,
            :leadTime,
            :modelName,
            :modelVersion,
            :dataValue,
            :dataUnit,
            :dataLocation,
            :dataSeries,
            :dataDatum
        )
        ON CONFLICT ON CONSTRAINT "outputs_AK00" DO NOTHING
        RETURNING *;
        """)
        bind_params = {
            'timeGenerated': row_to_insert["timeGenerated"],
            'leadTime': row_to_insert["leadTime"],
            'modelName': row_to_insert["modelName"],
            'modelVersion': row_to_insert["modelVersion"],
            'dataValue': row_to_insert["dataValue"],
            'dataUnit': row_to_insert["dataUnit"],
            'dataLocation': row_to_insert["dataLocation"],
            'dataSeries': row_to_insert["dataSeries"],
            'dataDatum': row_to_insert["dataDatum"]
        }
        stmt = stmt.bindparams(**bind_params)

        # insert the row into the outputs table returning what is inserted as a sanity check
        with self.__get_engine().connect() as conn:
            cursor = conn.execute(stmt)
            result = cursor.fetchone()
            conn.commit()
        
        """
        TODO:: When we reach implementing CRPS, we may not want to return
        the result series because of how much data will be a part of the
        result data frame. 
        """

        # create a series object to return with the inserted data
        # or an empty data frame if nothing was inserted due to a conflict with an existing row
        resultSeries = Series(series.description, series.timeDescription)
        if result is None:
            id = None
            resultSeries.dataFrame = get_output_dataFrame()
        else:
            # turn tuple objects into actual objects
            resultSeries.dataFrame = self.__splice_output([result])
            id = result[0]
        return resultSeries, id

    def fetch_oldest_generated_time(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> datetime | None:
        """
        Returns the oldest generated time within a time window.

        Data Assumptions (in the inputs table):
        - Every verifiedTime in [from_dt, to_dt] has at least one row in the database.
        - It’s acceptable to treat the ensemble as fresh if any member is fresh.
        - If the worst-case generatedTime is fresh, you assume all verifiedTimes are fresh enough.
        - Freshness is evaluated per verifiedTime, not per ensemble member.
        
           
        Query Summary: 
        Groups all rows by verifiedTime and, for each group, selects the latest generatedTime.
        From this set of “latest-per-verifiedTime” timestamps, the oldest one is then selected and returned.
        We don’t group by ensemble member ID here because, for staleness, we only care about the freshest run per verifiedTime overall. 
        Grouping by ensemble member ID would only be necessary if we needed the latest generatedTime per verifiedTime and ensembleMemberId
        rather than a single timestamp per verifiedTime.(Note that all ensemble groups with the same verified time are compared against eachother using this method.)
                
        Expected attributes:
        :param seriesDescription: SeriesDescription - A series description object
        :param timeDescription: TimeDescription - A hydrated time description object
        """

        query_stmt = text(f"""
        WITH latest_per_group AS (
            SELECT DISTINCT ON ("verifiedTime", "ensembleMemberID")
                i."generatedTime",
                i."verifiedTime",
                i."ensembleMemberID"
            FROM inputs AS i
            WHERE 	
				i."dataSource"   = :dataSource
                AND i."dataLocation" = :dataLocation
                AND i."dataSeries"   = :dataSeries
                AND {'i."dataDatum" = :dataDatum' if seriesDescription.dataDatum is not None else 'i."dataDatum" IS NULL'}
                AND i."verifiedTime" BETWEEN :from_dt AND :to_dt
            ORDER BY
                i."verifiedTime",
                i."ensembleMemberID",
                i."generatedTime" DESC
        )
        SELECT
            MIN("generatedTime") OVER () AS "generatedTime"
        FROM latest_per_group
        ORDER BY
            "verifiedTime",
            "ensembleMemberID"
		LIMIT 1
        """)
        bind_params = {
            'dataSource': seriesDescription.dataSource,
            'dataLocation': seriesDescription.dataLocation,
            'dataSeries': seriesDescription.dataSeries,
            'from_dt': timeDescription.fromDateTime,
            'to_dt': timeDescription.toDateTime,
        }
        if seriesDescription.dataDatum is not None:
            bind_params['dataDatum'] = seriesDescription.dataDatum
        query_stmt = query_stmt.bindparams(**bind_params)
        
        tupleishResult = self.__dbSelection(query_stmt).fetchall()
        
        # Data is not present in the DB
        if not tupleishResult or not tupleishResult[0][0]: 
            return None
        
        oldestGeneratedTime = pd.to_datetime(tupleishResult[0][0]).tz_localize(timezone.utc)

        return oldestGeneratedTime
       
    def fetch_row_with_max_verified_time_in_range(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> tuple | None:
        """
        This function returns the row with the max verified time in the requested range

        params:
            seriesDescription: SeriesDescription - A series description object
            timeDescription: TimeDescription - A hydrated time description object

        Returns:
            tuple | None - The row with the max verified time in the requested range or None if no data is found

        The returned tuple will have the order of:
        (id, generatedTime, acquiredTime, verifiedTime, dataValue, isActual, dataUnit, dataSource, dataLocation,
        dataSeries, dataDatum, latitude, longitude, ensembleMemberID)
        """

        # this query gets the row with the max verified time in the requested range
        query_stmt = text(f"""
        SELECT  
        i.*
        FROM inputs AS i
        WHERE i."dataSource"   = :dataSource
        AND i."dataLocation" = :dataLocation
        AND i."dataSeries"   = :dataSeries
        AND {'i."dataDatum" = :dataDatum' if seriesDescription.dataDatum is not None else 'i."dataDatum" IS NULL'}
        AND i."verifiedTime" BETWEEN :from_dt AND :to_dt
        ORDER BY i."verifiedTime" DESC
        LIMIT 1;
        """)
        bind_params = {
            'dataSource': seriesDescription.dataSource,
            'dataLocation': seriesDescription.dataLocation,
            'dataSeries': seriesDescription.dataSeries,
            'from_dt': timeDescription.fromDateTime,
            'to_dt': timeDescription.toDateTime
        }

        # only bind dataDatum if it's not None
        if seriesDescription.dataDatum is not None:
            bind_params['dataDatum'] = seriesDescription.dataDatum
        
        query_stmt = query_stmt.bindparams(**bind_params)
        
        tupleishResult = self.__dbSelection(query_stmt).fetchall()
        
        # convert the list of tuples to a single tuple and return it, else return None
        return tuple(tupleishResult[0]) if tupleishResult else None

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

        # --- Normalize dtypes and ADD TIMEZONE INFO ---
        df_results["generatedTime"] = pd.to_datetime(df_results["generatedTime"], errors="coerce").dt.tz_localize(timezone.utc)
        df_results["verifiedTime"] = pd.to_datetime(df_results["verifiedTime"], errors="coerce").dt.tz_localize(timezone.utc)
        df_results["acquiredTime"] = pd.to_datetime(df_results["acquiredTime"], errors="coerce").dt.tz_localize(timezone.utc)

        # A formatted dataframe to place the spliced data 
        df_out = get_input_dataFrame()

        # We have to post process the results as this could be ensemble data or not
        # grouping by verifiedTime groups the data by time point
        for _, group in df_results.groupby(["verifiedTime", "generatedTime"]): # group by verified time and generated time to prevent collapsing when there are multiple generated times for one verified time

            # ensembleMemberID is None if not an ensemble, so we check the first row of the group
            firstRow = group.iloc[0]
            isEnsemble = firstRow["ensembleMemberID"] is not None
            
            # If its an ensemble the dataValue is all the values sorted by there ID (to ensure correct order)
            # If not an ensemble we just take the solitary dataValue
            if isEnsemble:
                group = group.sort_values(by=("ensembleMemberID"))
                dataValues = group["dataValue"].to_list()
                
                # Pack the data into the out dataframe
                df_out.loc[len(df_out)] = [
                    dataValues,                   # dataValue
                    firstRow["dataUnit"],         # dataUnit
                    firstRow["verifiedTime"],     # timeVerified
                    firstRow["generatedTime"],    # timeGenerated
                    firstRow["longitude"],        # longitude
                    firstRow["latitude"]          # latitude
                ]
            
            else:
                for _, row in group.iterrows():
                    df_out.loc[len(df_out)] = [
                        row["dataValue"],  # full list for ensembles; scalar otherwise
                        row["dataUnit"],         # dataUnit
                        row["verifiedTime"],     # timeVerified
                        row["generatedTime"],    # timeGenerated
                        row["longitude"],        # longitude
                        row["latitude"]          # latitude
                    ]
            
        return df_out

    def __splice_output(self, results: list[tuple]) -> DataFrame:
        """
        Converts DB row results into a proper output dataframe to be packed into a series.

        :param results - list[tupleish] - a list of selected rows from the table formatted in tupleish.
            On model runs, there should only be one row in the list such as 
            [(ID, timeGenerated, leadTime, modelName, modelVersion, dataValue, dataUnit, dataLocation, dataSeries, dataDatum)]
            
            On output selections, there may be many rows returned from the DB with each row having the columns listed above.

        :returns: DataFrame - a dataframe with the data formatted for use in a series where each row
            of this dataframe corresponds to a single row in the database aka a single model run.

            On model runs, the dataframe will have only 1 row with the columns
            [dataValue, dataUnit, timeGenerated, leadTime]

            On output selections, the dataframe may have many rows but will still have the columns above.

            In both cases, the returned dataValue column for each row is expected to be in serialized format
            where we must deserialize the bytes back into an ndarray.
        """

        # Convert returned DB rows into a dataframe to make manipulation easier 
        df_results = pd.DataFrame(
            data=results, 
            columns=[
                "ID", "timeGenerated", "leadTime", "modelName", "modelVersion", "dataValue", 
                "dataUnit", "dataLocation", "dataSeries", "dataDatum"
            ]
        )

        if df_results.empty:
            return get_output_dataFrame()
        
        # --- ADD TIMEZONE INFO to timeGenerated ---
        df_results["timeGenerated"] = pd.to_datetime(df_results["timeGenerated"], errors="coerce").dt.tz_localize(timezone.utc)

        # A formatted dataframe to place the spliced data 
        df_out = get_output_dataFrame()

        """
        TODO:: This currently deserializes the dataValue for each row in the data frame one at a time.
        For queries that return many rows like select_output, this could be a performance issue.
        After implementing this, we should do some profiling on this and potentially deserialize 
        in a vectorized way by deserializing many items at once.
        """

        for idx, row in df_results.iterrows():
            # deserialize the data back into an ndarray
            deserialized_data = self.__deserialize_data(row["dataValue"])
            
            # Pack the data into the out dataframe
            df_out.loc[len(df_out)] = [
                deserialized_data,     # dataValue    
                row["dataUnit"],       # dataUnit
                row["timeGenerated"],  # timeGenerated
                row["leadTime"]        # leadTime
            ]

        return df_out

    def __serialize_data(self, array: np.ndarray | None) -> bytes | None:
        """
        This method serializes an ndarray into bytes.
        
        :param array: np.ndarray | None - The array to serialize or None on run fails
            The array is expected to have three dimensions of (members, inputs, outputs) and have 
            one of the following shapes depending on the type of model:
                (1, 1, 1) for single point models
                (1, 100, 1) for MRE models
                (10, 100, 100) for CRPS models
        
        :returns bytes | None - The serialized array in bytes or None if the input array was None
        """
        if array is None:
            return None
        
        buffer = BytesIO()
        np.save(buffer, array, allow_pickle=False)
        serialized_array = buffer.getvalue()
        buffer.close()

        return serialized_array

    def __deserialize_data(self, serialized_data: bytes | None) -> np.ndarray | None:
        """
        This method deserializes bytes into an ndarray.

        :param serialized_data: bytes | None - The bytes to deserialize
            or None if a null was returned from the db which means the original array was None before serialization.

        :returns ndarray | None - The reconstructed ndarray before it was serialized and stored
            or None if the original array was None before it was inserted.
        """
        if serialized_data is None:
            return None
        
        buffer = BytesIO(serialized_data)
        array = np.load(buffer, allow_pickle=False)
        buffer.close()

        return array
    
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