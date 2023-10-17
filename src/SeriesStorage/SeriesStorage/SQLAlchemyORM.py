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
from sqlalchemy import Table, Column, Integer, String, DateTime, Float, MetaData, UniqueConstraint, Engine, ForeignKey, insert, CursorResult, Select, select, distinct
from os import getenv
from datetime import timedelta, datetime

from src.SeriesStorage.ISeriesStorage import ISeriesStorage

from DataClasses import Series, SeriesDescription, SemaphoreSeriesDescription, Input, Output, TimeDescription

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

    def select_input(self, seriesDescription: SeriesDescription, timeDescription : TimeDescription) -> Series:
        """Selects a given series given a SeriesDescription and TimeDescription"""

        statement = (select(self.inputs)
            .where(self.inputs.c.dataSource == seriesDescription.dataSource)
            .where(self.inputs.c.dataLocation == seriesDescription.dataLocation)
            .where(self.inputs.c.dataSeries == seriesDescription.dataSeries)
            .where(self.inputs.c.dataDatum == seriesDescription.dataDatum)
            .where(self.inputs.c.verifiedTime >= timeDescription.fromDateTime)
            .where(self.inputs.c.verifiedTime <= timeDescription.toDateTime)
            )
        tupleishResult = self.__dbSelection(statement).fetchall()
        inputResult = self.__splice_input(tupleishResult)
        series = Series(seriesDescription, True, timeDescription)
        series.data = inputResult
        return series
    
    def select_output(self, semaphoreSeriesDescription: SemaphoreSeriesDescription, timeDescription : TimeDescription) -> Series:
        
        statement = (select(distinct(self.outputs.c.leadTime))
                    .where(self.outputs.c.dataSource == semaphoreSeriesDescription.dataSource)
                    .where(self.outputs.c.dataLocation == semaphoreSeriesDescription.dataLocation)
                    .where(self.outputs.c.dataSeries == semaphoreSeriesDescription.dataSeries)
                    .where(self.outputs.c.dataDatum == semaphoreSeriesDescription.dataDatum)
                    )
        leadTime = self.__dbSelection(statement).fetchall()[0]
        fromGeneratedTime = timeDescription.fromDateTime - leadTime
        toGeneratedTime = timeDescription.toDateTime - leadTime
         
        statement = (select(self.outputs)
                    .where(self.outputs.c.dataSource == semaphoreSeriesDescription.dataSource)
                    .where(self.outputs.c.dataLocation == semaphoreSeriesDescription.dataLocation)
                    .where(self.outputs.c.dataSeries == semaphoreSeriesDescription.dataSeries)
                    .where(self.outputs.c.dataDatum == semaphoreSeriesDescription.dataDatum)
                    .where(self.outputs.c.timeGenerated >= fromGeneratedTime)
                    .where(self.outputs.c.timeGenerated <= toGeneratedTime)
           )
        tupleishResult = self.__dbSelection(statement).fetchall()
        outputResult = self.__splice_output(tupleishResult)
        series = Series(semaphoreSeriesDescription, True, timeDescription)
        series.data = outputResult
        return series
    
    def find_external_location_code(self, sourceCode: str, location: str, priorityOrder: int = 0) -> str:
        raise NotImplementedError()

    def find_lat_lon_coordinates(self, sourceCode: str, location: str, priorityOrder: int = 0) -> tuple:
        raise NotImplementedError()

    def insert_input(self, series: Series) -> Series:
        """This method inserts actual/predictions into the input table
            :param series: Series - A series object with a time description, series description, and input data
            :return Series - A series object that contains the actually inserted  data
        """
     #Construct DB row to insert
        now = datetime.now()
        insertionRows = []
        for input in series.data:
            insertionValueRow = {"generatedTime": None, "aquiredTime": None, "verifiedTime": None, "dataValue": None, "isActual": None, "dataUnit": None, "dataSource": None, "dataLocation": None, "dataDatum": None, "latitude": None, "longitude": None}
            insertionValueRow["generatedTime"] = input.timeGenerated
            insertionValueRow["acquiredtime"] = now
            insertionValueRow["timeVerified"] = input.timeVerified
            insertionValueRow["dtaaValue"] = input.dataValue
            insertionValueRow["isActual"] = False if series.description.dataSeries[0] == 'p' else True
            insertionValueRow["dataUnit"] = input.dataUnit
            insertionValueRow["dataSource"] = series.description.dataSource
            insertionValueRow["dataLocation"] = series.description.dataLocation
            insertionValueRow["dataSeries"] = series.description.dataSeries
            insertionValueRow["dataDatum"] = series.description.dataDatum
            insertionValueRow["latitude"] = input.latitude
            insertionValueRow["longitude"] = input.longitude
            insertionRows.append(insertionValueRow)

        with self.get_engine().connect() as conn:
            cursor = conn.execute(insert(self.inputs)
                                .returning(self.inputs)
                                .values(insertionRows)
                                .prefix_with('OR IGNORE')
                                )
            result = cursor.fetchall()
            conn.commit()

        resultSeries = Series(series.description, True, series.timeDescription)
        resultSeries.data = self.__splice_input(result) #Turn tuple objects into actual objects
        return resultSeries
    


    def insert_output(self, series: Series) -> Series:
        """This method inserts actual/predictions into the output table
            :param series: Series - A series object with a time description,  semaphore series description, and outputdata
            :return Series - A series object that contains the actually inserted data
        """
        insertionValueRow = []
        for output in series.data:
            insertionValueRow = {"timeGenerated": None, "leadTime": None, "modelName": None, "dataValue": None, "dataUnit": None, "dataLocation": None, "dataSeries": None, "dataDatum": None}
            insertionValueRow["timeGenerated"] = output.timeGenerated
            insertionValueRow["leadTime"] = output.leadTime
            insertionValueRow["modelName"] = series.description.modelName
            insertionValueRow["modelVersion"] = series.description.modelVersion
            insertionValueRow["dataValue"] = output.dataValue
            insertionValueRow["dataUnit"] = output.dataUnit
            insertionValueRow["dataLocation"] = series.description.dataLocation
            insertionValueRow["dataSeries"] = series.description.dataSeries
            insertionValueRow["dataDatum"] = series.description.dataDatum
            
            insertionValueRow.append(insertionValueRow)

        with self.get_engine().connect() as conn:
            cursor = conn.execute(insert(self.outputs)
                                  .returning(self.outputs)
                                  .values(insertionValueRow)
                                  .prefix_with('OR IGNORE')
                                  )
            result = cursor.fetchall()
            conn.commit()

        resultSeries = Series(series.description, True, series.timeDescription)
        resultSeries.data = self.__splice_output(result) #Turn tuple objects into actual objects
        return resultSeries
    

    def create_DB(self) -> None:
        """Creates the database with the tethered engine.
        Requires the engine to be created before it will create the DB.
        See: DBManager.create_engine()
        """

        self._metadata.create_all(self.__get_engine())
     

    def drop_DB(self) -> None:
        """Drops the database with the tethered engine.
        Requires the engine to be created before it will drop the DB.
        See: DBManager.create_engine()
        """

        self._metadata.drop_all(self.__get_engine())

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

    def __splice_input(self, results: list[tuple]) -> list[Input]:
        """An Input is a data value of some environment variable that can be linked to a date time.
        :param list[tupleish] -a list of selections from the table formatted in tupleish
        """
        valueIndex = 4
        unitIndex = 6
        generatedTimeIndex = 1
        verifiedTimeIndex = 3
        latitudeIndex = 11
        longitudeIndex = 12

        dataPoints = []
        for row in results:
            dataPoints.append(Input(
                row[valueIndex],
                row[unitIndex],
                row[generatedTimeIndex],
                row[verifiedTimeIndex],
                row[latitudeIndex],
                row[longitudeIndex]
            ))

        return dataPoints

    def __splice_output(self, results: list[tuple]) -> list[Output]:
        """Splices up a list of DB results, pulling out only the data that changes per point,
        and places them in a Prediction object.
        param: list[tupleish] - a list of selections from the table formatted in tupleish
        """
        valueIndex = 5
        unitIndex = 6
        timeGeneratedIndex = 1
        leadTimeIndex = 2
        
        dataPoints = []
        for row in results:
            dataPoints.append(Output(
                row[valueIndex],
                row[unitIndex],
                row[timeGeneratedIndex],
                row[leadTimeIndex]
            ))

        return dataPoints
