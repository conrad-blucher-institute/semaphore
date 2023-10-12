# -*- coding: utf-8 -*-
#SQLAlchemyORM.py
#-------------------------------
# Created By : Matthew Kastl
# Created Date: 3/26/2023
# version 7.0
#-------------------------------
""" This file is an implementation of the SQLAlchemy ORM geared towards Semaphore and its schema. 
 """ 
#-------------------------------
# 
#
#Imports
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from sqlalchemy import create_engine as sqlalchemy_create_engine
from sqlalchemy import Table, Column, Integer, String, DateTime, Float, MetaData, UniqueConstraint, Engine, ForeignKey, insert, CursorResult, Select, select
from os import getenv
from datetime import timedelta

from src.SeriesStorage.ISeriesStorage import ISeriesStorage

from DataClasses import *

class SQLAlchemyORM(ISeriesStorage):
 
    def __init__(self) -> None:
        """Constructor generates an a db schema. Automatically creates the 
        metadata object holding the defined schema.
        """
        self.__create_schema()
        self.__create_engine(getenv('DB_LOCATION_STRING'), False)

    #############################################################################################
    ################################################################################## Public methods
    #############################################################################################

    def create_DB(self) -> None:
        """Creates the database with the tethered engine.
        Requires the engine to be created before it will create the DB.
        See: DBManager.create_engine()
        """

        self._metadata.create_all(self.get_engine())
     

    def drop_DB(self) -> None:
        """Drops the database with the tethered engine.
        Requires the engine to be created before it will drop the DB.
        See: DBManager.create_engine()
        """

        self._metadata.drop_all(self.get_engine())

    #############################################################################################
    ################################################################################## DB Managment Methods
    #############################################################################################

    def __create_engine(self, parmaString: str, echo: bool ) -> None: #"sqlite+pysqlite:///:memory:"
        """Creates an engine object and tethers it to this interface class as an atribute

        Parameters:
            permaString: str - An sqlalchemy string that defines the location the engine should point to: (e.g. "sqlite+pysqlite:///:memory:")
            echo: str - Weather or not the engine should echo to stdout
        """
        self._engine = sqlalchemy_create_engine(parmaString, echo=echo)

    
    def __get_engine(self) -> Engine:
        """Fetches the engine attribute. Requires the engine attribute to be created.
        See: DBManager.create_engine()
        """

        if not hasattr(self, '_engine') or self._engine == None:
            raise Exception("An engine was requested from DBManager, but no engine has been created. See DBManager.create_engine()")
        else:
            return self._engine

    def __create_schema(self) -> None:
        """Builds the db schema in the metadata.
        """

        self._metadata = MetaData()
        
        #this table stores the actual data values as retrieved or received 
        self.inputs = Table(
            "inputs",
            self._metadata,

            Column("id", Integer, autoincrement=True, primary_key=True),

            Column("isActual", bool, nullable=False),

            Column("generatedTime", DateTime, nullable=True),
            Column("acquiredTime", DateTime, nullable=False),
            Column("verifiedTime", DateTime, nullable=False), 

            Column("dataUnit", String(10), ForeignKey("ref_dataUnit.code"), nullable=False),   
            Column("dataSource", String(10), ForeignKey("ref_dataSource.code"), nullable=False),
            Column("dataLocation", String(25), ForeignKey("ref_dataLocation.code"), nullable=False), 
            Column("dataSeries", String(10), ForeignKey("ref_dataSeries.code"), nullable=False), 
            Column("dataDatum", String(10), ForeignKey("ref_dataDatum.code"),  nullable=True),
            Column("latitude", String(16), nullable=True),
            Column("longitude", String(16), nullable=True),

            UniqueConstraint("isActual", "generatedTime", "verifiedTime", "dataUnit", "dataSource", "dataLocation", "dataSeries", "dataDatum", "latitude", "longitude"),
        )

        
        self.outputs = Table(
            "outputs",
            self._metadata,

            Column("id", Integer, autoincrement=True, primary_key=True),
            
            Column("timeGenerated", DateTime, nullable=False),

            Column("leadTime", timedelta, nullable=False),

            Column("modelName", String(25), nullable=False), 
            Column("modelVersion", String(10), nullable=False),
            Column("dataValue", String(20), nullable=False), 
            Column("dataUnit", String(10), ForeignKey("ref_dataUnit.code"), nullable=False), 
            Column("dataLocation", String(25), ForeignKey("ref_dataLocation.code"), nullable=False),   
            Column("dataSeries", String(10), ForeignKey("ref_dataSeries.code"), nullable=False),         
            Column("dataDatum", String(10), ForeignKey("ref_dataDatum.code"), nullable=True),

            UniqueConstraint("timeGenerated", "leadTime", "modelName", "modelVersion", "dataLocation", "dataSeries", "dataDatum"),
        )

        #This table maps CBI location codes to location codes used by datasorces
        self.dataLocation_dataSource_mapping = Table(
            "dataLocation_dataSource_mapping",
            self._metadata,

            Column("id", Integer, autoincrement=True, primary_key=True),
            
            Column("dataLocationCode", String(25), ForeignKey("ref_dataLocation.code"),nullable=False),  
            Column("dataSourceCode", String(10), ForeignKey("ref_dataSource.code"), nullable=False), 
            Column("dataSourceLocationCode", String(255), nullable=False),                            
            Column("priorityOrder", Integer, nullable=False),                                         

            UniqueConstraint("dataLocationCode", "dataSourceCode", "dataSourceLocationCode", "priorityOrder"),
        )


        #The rest of these tables are reference tables for values stored in the tables above. They all contain
        # ID - Automated id
        # code - that mapped code
        # display name - a non compressed pretty name
        # notes - more information about that item
        self.ref_dataLocation = Table(
            "ref_dataLocation",
            self._metadata,

            Column("id", Integer, autoincrement=True, primary_key=True),
            
            Column("code", String(25), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),
            Column("latitude", String(16), nullable=False),
            Column("longitude", String(16), nullable=False),

            UniqueConstraint("code", "displayName"),
        )

        self.ref_dataSource = Table(
            "ref_dataSource",
            self._metadata,

            Column("id", Integer, autoincrement=True, primary_key=True),
            
            Column("code", String(10), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )

        self.ref_dataSeries = Table(
            "ref_dataSeries",
            self._metadata,

            Column("id", Integer, autoincrement=True, primary_key=True),
            
            Column("code", String(10), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )

        self.ref_dataUnit = Table(
            "ref_dataUnit",
            self._metadata,

            Column("id", Integer, autoincrement=True, primary_key=True),
            
            Column("code", String(10), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )

        self.ref_dataDatum = Table(
            "ref_dataDatum",
            self._metadata,

            Column("id", Integer, autoincrement=True, primary_key=True),
            
            Column("code", String(10), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )

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

        with self.get_engine().connect() as conn:
            result = conn.execute(stmt)

        return result
    
    def __splice_prediction_results(self, results: List[tuple]) -> List[Prediction]:
        """Splices up a list of dbresults, pulling out only the data that changes per point,
        and places them in a Prediction object.
        Parameters:
            first list[tupleish (sqlalchemy.engine.row.Row)] - The collection of dbrows.
        Returns:
            List[Prediction] - The formatted objs.
        """
        valueIndex = 3
        unitIndex = 4
        leadTimeIndex = 2
        timeGeneratedIndex = 1
        resultCodeIndex = 6
        latitudeIndex = 11
        longitudeIndex = 12
        predictions = []
        for row in results:
            predictions.append(Prediction(
                row[valueIndex],
                row[unitIndex],
                row[leadTimeIndex],
                row[timeGeneratedIndex],
                row[resultCodeIndex],
                row[longitudeIndex],
                row[latitudeIndex]
            ))

        return predictions
    
    def __splice_output_prediction_results(self, results: List[tuple]) -> List[Output]:
        """Splices up a list of dbresults, pulling out only the data that changes per point,
        and places them in a Prediction object.
        Parameters:
            first list[tupleish (sqlalchemy.engine.row.Row)] - The collection of dbrows.
        Returns:
            List[Prediction] - The formatted objs.
        """
        valueIndex = 6
        unitIndex = 7
        leadTimeIndex = 3
        timeGeneratedIndex = 2
        predictions = []
        for row in results:
            predictions.append(Output(
                row[valueIndex],
                row[unitIndex],
                row[leadTimeIndex],
                row[timeGeneratedIndex]
            ))

        return predictions
        

    def __splice_actual_results(self, results: List[tuple]) -> List[Actual]:
        """Splices up a list of dbresults, pulling out only the data that changes per point,
        and places them in a DataPoint object.
        -------
        Parameters:
            first list[tupleish (sqlalchemy.engine.row.Row)] - The collection of dbrows.
        Returns:
            List[Actual] - The formatted objs.
        """
        valueIndex = 3
        unitIndex = 4
        timeActualizedIndex = 1
        longitudeIndex = 11
        latitudeIndex = 10
        dataPoints = []
        for row in results:
            dataPoints.append(Actual(
                row[valueIndex],
                row[unitIndex],
                row[timeActualizedIndex],
                row[longitudeIndex],
                row[latitudeIndex]
            ))

        return dataPoints
    

    def __splice_output_table_results(self, results: List[tuple]) -> List[Prediction]:
        """Splices up a list of dbresults, pulling out only the data that changes per point,
        and places them in a Prediction object.
        -------
        -------
        Parameters:
            first list[tupleish (sqlalchemy.engine.row.Row)] - The collection of dbrows.
        Returns:
            List[Prediction] - The formatted objs.
        """
        valueIndex = 6
        unitIndex = 7
        leadTimeIndex = 3
        timeGeneratedIndex = 2
        dataPoints = []
        for row in results:
            dataPoints.append(Prediction(
                row[valueIndex],
                row[unitIndex],
                row[leadTimeIndex],
                row[timeGeneratedIndex]
            ))

        return dataPoints

    #############################################################################################
    ################################################################################## Public selection methods
    #############################################################################################
    
    def s_data_point_selection(self, seriesDescription: SeriesDescription) -> Series:
        """Selects from the data point table.
        Parameter:
            seriesDescription
        Returns:
            Series
        """
        table = self.s_data_point
        stmt = (select(table)
            .where(table.c.dataSourceCode == seriesDescription.source)
            .where(table.c.sLocationCode == seriesDescription.location)
            .where(table.c.seriesCode == seriesDescription.series)
            .where(table.c.interval == seriesDescription.interval)
            .where(table.c.datumCode == seriesDescription.datum)
            .where(table.c.timeActualized >= seriesDescription.fromDateTime)
            .where(table.c.timeActualized <= seriesDescription.toDateTime)
        )
        
        result = self.__dbSelection(stmt).fetchall()
        resultSeries = Series(seriesDescription, True)
        resultSeries.data = self.__splice_actual_results(result) #Turn tuple objects into prediction objects
        return resultSeries
    
    
    def s_prediction_selection(self, seriesDescription: SeriesDescription) -> Series:
        """Selects from the prediction table.
        Parameter:
            seriesDescription
        Returns:
            Series
        """

        table = self.s_prediction
        stmt = (select(table)
            .where(table.c.dataSourceCode == seriesDescription.source)
            .where(table.c.sLocationCode == seriesDescription.location)
            .where(table.c.seriesCode == seriesDescription.series)
            .where(table.c.interval == seriesDescription.interval)
            .where(table.c.datumCode == seriesDescription.datum)
            .where(table.c.timeGenerated >= seriesDescription.fromDateTime)
            .where(table.c.timeGenerated <= seriesDescription.toDateTime)
        )

        result = self.__dbSelection(stmt).fetchall()
        resultSeries = Series(seriesDescription, True)
        resultSeries.data = self.__splice_prediction_results(result) #Turn tuple objects into prediction objects
        return resultSeries
        
    def select_s_output(self, seriesDescription) -> Series:

        #offset the the time range by the lead time, as the time range is in verification time
        #but the objects are stored by generated time
        leadDelta = timedelta(hours= seriesDescription.leadTime)
        fromDateTime = seriesDescription.fromDateTime - leadDelta
        toDateTime = seriesDescription.toDateTime - leadDelta

        table = self.s_prediction_output
        stmt = (select(table)
                .where(table.c.leadTime == seriesDescription.leadTime)
                .where(table.c.ModelName == seriesDescription.ModelName)
                .where(table.c.ModelVersion == seriesDescription.ModelVersion)
                .where(table.c.timeGenerated >= fromDateTime)
                .where(table.c.timeGenerated <= toDateTime)
                )
                
        result = self.__dbSelection(stmt).fetchall()
        resultSeries = Series(seriesDescription, True)
        resultSeries.data = self.__splice_output_prediction_results(result) #Turn tuple objects into prediction objects
        return resultSeries

    def s_locationCode_dataSourceLocationCode_mapping_select(self, sourceCode: str, location: str, priorityOrder: int = 0) -> list[tuple]:
        """Selects a a dataSourceLocationCode given a datasource and a location. 
        ----
        Returns list[tupleish (sqlalchemy.engine.row.Row)]
        """
        table = self.s_locationCode_dataSourceLocationCode_mapping
        stmt = (select(table)
                .where(table.c.dataSourceCode == sourceCode)
                .where(table.c.sLocationCode == location)
                .where(table.c.priorityOrder == priorityOrder)
                )

        return self.__dbSelection(stmt).fetchall()

    #############################################################################################
    ################################################################################## Purblic insertion Methods
    #############################################################################################

    def s_data_point_insert(self, series: Series) -> Series:
        """Inserts a series into s_data_point
        Parameter:
            Series - The data to insert.
        Returns:
            Series - The data actually inserted.
        """

        #Construct DB row to insert
        now = datetime.now()
        insertionRows = []
        for actual in series.data:
            insertionValueRow = {"timeActualized": None, "timeAquired": None, "dataValue": None, "unitsCode": None, "interval": 0, "dataSourceCode": None, "sLocationCode": None, "seriesCode": None, "datumCode": None, "latitude": None, "longitude": None}
            insertionValueRow["timeActualized"] = actual.dateTime
            insertionValueRow["timeAquired"] = now
            insertionValueRow["dataValue"] = actual.value
            insertionValueRow["unitsCode"] = actual.unit
            insertionValueRow["interval"] = series.description.interval
            insertionValueRow["dataSourceCode"] = series.description.source
            insertionValueRow["sLocationCode"] = series.description.location
            insertionValueRow["seriesCode"] = series.description.series
            insertionValueRow["datumCode"] = series.description.datum
            insertionValueRow["latitude"] = actual.latitude
            insertionValueRow["longitude"] = actual.longitude
            insertionRows.append(insertionValueRow)

        with self.get_engine().connect() as conn:
            cursor = conn.execute(insert(self.s_data_point)
                                  .returning(self.s_data_point)
                                  .values(insertionRows)
                                  .prefix_with('OR IGNORE')
                                  )
            result = cursor.fetchall()
            conn.commit()

        resultSeries = Series(series.description, True)
        resultSeries.data = self.__splice_actual_results(result) #Turn tuple objects into actual objects
        return resultSeries
    


    def s_prediction_insert(self, series: Series) -> Series:
        """Inserts a series into s_prediction
        Parameter:
            Series - The data to insert.
        Returns:
            Series - The data actually inserted.
        """

        insertionRows = []
        for prediction in series.data:
            insertionValueRow = {"timeGenerated": None, "leadTime": None, "dataValue": None, "unitsCode": None, "interval": 0, "resultCode": None, "dataSourceCode": None, "sLocationCode": None, "seriesCode": None, "datumCode": None, "latitude": None, "longitude": None}
            insertionValueRow["timeGenerated"] = prediction.generatedTime
            insertionValueRow["leadTime"] = prediction.leadTime
            insertionValueRow["dataValue"] = prediction.value
            insertionValueRow["unitsCode"] = prediction.unit
            insertionValueRow["interval"] = series.description.interval
            insertionValueRow["resultCode"] = prediction.successValue
            insertionValueRow["dataSourceCode"] = series.description.source
            insertionValueRow["sLocationCode"] = series.description.location
            insertionValueRow["seriesCode"] = series.description.location
            insertionValueRow["datumCode"] = series.description.datum
            insertionValueRow["latitude"] = prediction.latitude
            insertionValueRow["longitude"] = prediction.longitude
    
            insertionRows.append(insertionValueRow)

        with self.get_engine().connect() as conn:
            cursor = conn.execute(insert(self.s_prediction)
                                  .returning(self.s_prediction) 
                                  .values(insertionRows)
                                  .prefix_with('OR IGNORE')
                                  )
            result = cursor.fetchall()
            conn.commit()

        resultSeries = Series(series.description, True)
        resultSeries.description = series.description
        resultSeries.data = self.__splice_prediction_results(result) #Turn tuple objects into prediction objects
        return resultSeries
    


    def s_prediction_output_insert(self, series: Series) -> Series:
        """Inserts a series into s_prediction_output
        Parameter:
            Series - The data to insert.
        Returns:
            Series - The data actually inserted.
        """

        now = datetime.now()
        insertionRows = []
        for prediction in series.data:

            #Consturct DB row to insert
            insertionValueRow = {"timeAquired": None, "timeGenerated": None, "leadTime": None, "ModelName": None, "ModelVersion": None, "dataValue": None, "unitsCode": None, "sLocationCode": None, "seriesCode": None, "datumCode": None}
            insertionValueRow["timeAquired"] = now
            insertionValueRow["timeGenerated"] = prediction.generatedTime
            insertionValueRow["leadTime"] = prediction.leadTime
            insertionValueRow["ModelName"] = series.description.ModelName
            insertionValueRow["ModelVersion"] = series.description.ModelVersion
            insertionValueRow["dataValue"] = str(prediction.value)
            insertionValueRow["unitsCode"] = prediction.unit
            insertionValueRow["sLocationCode"] = series.description.location
            insertionValueRow["seriesCode"] = series.description.series
            insertionValueRow["datumCode"] = series.description.datum
            insertionRows.append(insertionValueRow)

        with self.get_engine().connect() as conn:
            cursor = conn.execute(insert(self.s_prediction_output)
                                  .returning(self.s_prediction_output) 
                                  .values(insertionRows)
                                  .prefix_with('OR IGNORE')
                                  )
            result = cursor.fetchall()
            conn.commit()

        resultSeries = Series(series.description, True)
        resultSeries.description = series.description
        resultSeries.data = self.__splice_output_table_results(result) #Turn tuple objects into prediction objects
        return resultSeries
    

    def s_locationCode_dataSourceLocationCode_mapping_insert(self, values: dict | list[tuple]) -> list[tuple]:
        """Inserts a row or batch into s_locationCode_dataSourceLocationCode_mapping

        Dictionary reference: {"dataSourceCode", "sLocationCode", "dataSourceLocationCode", "priorityOrder"}

        Parameters:
            values: dict | list[tuple] - THe dictionary containing the insertion values (see dictionary reference above). Can either be one dictionary or a list of dictionaries.

        Returns:
            Returns list[tupleish (sqlalchemy.engine.row.Row)]
        """

        with self.get_engine().connect() as conn:
            cursor = conn.execute(insert(self.s_locationCode_dataSourceLocationCode_mapping)
                                  .returning(self.s_locationCode_dataSourceLocationCode_mapping), 
                                  values
                                  )
            result = cursor.fetchall()
            conn.commit()

        return result


    def s_ref_slocation_insert(self, values: dict | list[tuple]) -> list[tuple]:
        """Inserts a row or batch into s_ref_slocation

        Dictionary reference: {"code", "displayName", (OP)"notes"}

        Parameters:
            values: dict | list[tuple] - THe dictionary containing the insertion values (see dictionary reference above). Can either be one dictionary or a list of dictionaries.

        Returns:
            Returns list[tupleish (sqlalchemy.engine.row.Row)]
        """
        
        with self.get_engine().connect() as conn:
            cursor = conn.execute(insert(self.s_ref_slocation)
                                  .returning(self.s_ref_slocation), 
                                  values
                                  )
            result = cursor.fetchall()
            conn.commit()

        return result


    def s_ref_data_source_insert(self, values: dict | list[tuple]) -> list[tuple]:
        """Inserts a row or batch into s_ref_data_source

        Dictionary reference: {"code", "displayName", (OP)"notes"}

        Parameters:
            values: dict | list[tuple] - THe dictionary containing the insertion values (see dictionary reference above). Can either be one dictionary or a list of dictionaries.

        Returns:
            Returns list[tupleish (sqlalchemy.engine.row.Row)]
        """

        with self.get_engine().connect() as conn:
            cursor = conn.execute(insert(self.s_ref_data_source)
                                  .returning(self.s_ref_data_source), 
                                  values
                                  )
            result = cursor.fetchall()
            conn.commit()

        return result


    def s_ref_series_insert(self, values: dict | list[tuple]) -> list[tuple]:
        """Inserts a row or batch into s_ref_series

        Dictionary reference: {"code", "displayName", (OP)"notes"}

        Parameters:
            values: dict | list[tuple] - THe dictionary containing the insertion values (see dictionary reference above). Can either be one dictionary or a list of dictionaries.

        Returns:
            Returns list[tupleish (sqlalchemy.engine.row.Row)]
        """

        with self.get_engine().connect() as conn:
            cursor = conn.execute(insert(self.s_ref_series)
                                  .returning(self.s_ref_series), 
                                  values
                                  )
            result = cursor.fetchall()
            conn.commit()

        return result


    def s_ref_units_insert(self, values: dict | list[tuple]) -> list[tuple]:
        """Inserts a row or batch into s_ref_units

        Dictionary reference: {"code", "displayName", (OP)"notes"}

        Parameters:
            values: dict | list[tuple] - THe dictionary containing the insertion values (see dictionary reference above). Can either be one dictionary or a list of dictionaries.

        Returns:
            Returns list[tupleish (sqlalchemy.engine.row.Row)]
        """

        with self.get_engine().connect() as conn:
            cursor = conn.execute(insert(self.s_ref_units)
                                  .returning(self.s_ref_units), 
                                  values
                                  )
            result = cursor.fetchall()
            conn.commit()

        return result


    def s_ref_datum_insert(self, values: dict | list[tuple]) -> list[tuple]:
        """Inserts a row or batch into s_ref_datum

        Dictionary reference: {"code", "displayName", (OP)"notes"}

        Parameters:
            values: dict | list[tuple] - THe dictionary containing the insertion values (see dictionary reference above). Can either be one dictionary or a list of dictionaries.

        Returns:
            Returns list[tupleish (sqlalchemy.engine.row.Row)]
        """

        with self.get_engine().connect() as conn:
            cursor = conn.execute(insert(self.s_ref_datum)
                                  .returning(self.s_ref_datum), 
                                  values
                                  )
            result = cursor.fetchall()
            conn.commit()

        return result


    def s_ref_resultCode_insert(self, values: dict | list[tuple]) -> list[tuple]:
        """Inserts a row or batch into s_ref_resultCode

        Dictionary reference: {"code", "displayName", (OP)"notes"}

        Parameters:
            values: dict | list[tuple] - THe dictionary containing the insertion values (see dictionary reference above). Can either be one dictionary or a list of dictionaries.

        Returns:
            Returns list[tupleish (sqlalchemy.engine.row.Row)]
        """

        with self.get_engine().connect() as conn:
            cursor = conn.execute(insert(self.s_ref_resultCode)
                                  .returning(self.s_ref_resultCode), 
                                  values
                                  )
            result = cursor.fetchall()
            conn.commit()

        return result
    

