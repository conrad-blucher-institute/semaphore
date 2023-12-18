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
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from sqlalchemy import create_engine as sqlalchemy_create_engine
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, UniqueConstraint, Engine, ForeignKey, CursorResult, Select, select, distinct, Boolean, Interval, text
from sqlalchemy.dialects.postgresql import insert
from os import getenv
from datetime import timedelta, datetime

from src.SeriesStorage.ISeriesStorage import ISeriesStorage

from DataClasses import Series, SeriesDescription, SemaphoreSeriesDescription, Input, Output, TimeDescription
from utility import log

class SQLAlchemyORM_Postgres(ISeriesStorage):
 
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
        """Selects a given series given a SeriesDescription and TimeDescription
           :param seriesDescription: SeriesDescription - A series description object
           :param timeDescription: TimeDescription - A hydrated time description object
        """

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

        # If an interval was provided, we will mod each verified time against it
        # any that fail we remove
        if timeDescription.interval != None:
            for input in inputResult:
                if not (input.timeVerified.timestamp() % timeDescription.interval.total_seconds() == 0):
                    inputResult.remove(input)

        series = Series(seriesDescription, True, timeDescription)
        series.data = inputResult
        return series
    
    def select_output(self, semaphoreSeriesDescription: SemaphoreSeriesDescription, timeDescription : TimeDescription) -> Series:
        """Selects an output series given a SemaphoreSeriesDescription and TimeDescription
           :param semaphoreSeriesDescription: SemaphoreSeriesDescription - A  semaphore series description object
           :param timeDescription: TimeDescription - A hydrated time description object
        """

        series = Series(semaphoreSeriesDescription, True, timeDescription)
        

        #Get the lead time for time calculations
        statement = (select(distinct(self.outputs.c.leadTime))
                    .where(self.outputs.c.dataLocation == semaphoreSeriesDescription.dataLocation)
                    .where(self.outputs.c.dataSeries == semaphoreSeriesDescription.dataSeries)
                    .where(self.outputs.c.dataDatum == semaphoreSeriesDescription.dataDatum)
                    )
        leadTimes = self.__dbSelection(statement).fetchall()
        if len(self.__dbSelection(statement).fetchall()) == 0: #If no lead time is found for some reason return nothing and log this
            log(f'SQLAlchemyORM | select_output | No leadtime found for SemaphoreSeriesDescription:{semaphoreSeriesDescription}')
            return series
            
        leadTime = leadTimes[0]
        fromGeneratedTime = timeDescription.fromDateTime - leadTime[0]
        toGeneratedTime = timeDescription.toDateTime - leadTime[0]
         
        statement = (select(self.outputs)
                    .where(self.outputs.c.dataLocation == semaphoreSeriesDescription.dataLocation)
                    .where(self.outputs.c.dataSeries == semaphoreSeriesDescription.dataSeries)
                    .where(self.outputs.c.dataDatum == semaphoreSeriesDescription.dataDatum)
                    .where(self.outputs.c.timeGenerated >= fromGeneratedTime)
                    .where(self.outputs.c.timeGenerated <= toGeneratedTime)
                    )
        tupleishResult = self.__dbSelection(statement).fetchall()
        outputResult = self.__splice_output(tupleishResult)
        series.data = outputResult
        return series
    
    def find_external_location_code(self, sourceCode: str, location: str, priorityOrder: int = 0) -> str:
        """Returns a data source location code based off of passed parameters
           :param sourceCode: str - the data source code (noaaT&C)
           :param location: str - the local location name 
           :param priorityOrder: int - priority of which locations to go to if one is unavailable 
        """
        statement = (select(self.dataLocation_dataSource_mapping.c.dataSourceLocationCode)
                     .where(self.dataLocation_dataSource_mapping.c.dataSourceCode == sourceCode)
                     .where(self.dataLocation_dataSource_mapping.c.dataLocationCode == location)
                     .where(self.dataLocation_dataSource_mapping.c.priorityOrder == priorityOrder)
                    )
        dataSourceLocationCode = self.__dbSelection(statement).fetchall()[0]
        return dataSourceLocationCode[0]

    def find_lat_lon_coordinates(self, locationCode: str) -> tuple:
        """Returns lat and lon tuple
           :param sourceCode: str - the data source code (noaaT&C)
           :param location: str - the local location name 
           :param priorityOrder: int - priority of which locations to go to if one is unavailable 
        """
        statement = (select(self.ref_dataLocation.c.latitude, self.ref_dataLocation.c.longitude)
                     .where(self.ref_dataLocation.c.code == locationCode)
                    )
        latLon = self.__dbSelection(statement).first()
        return (latLon[0], latLon[1])
        

    def insert_input(self, series: Series) -> Series:
        """This method inserts actual/predictions into the input table
            :param series: Series - A series object with a time description, series description, and input data
            :return Series - A series object that contains the actually inserted  data
        """

        if(type(series.description).__name__ != 'SeriesDescription'): raise ValueError('Description should be type SeriesDescription')

     #  Construct DB row to insert
        now = datetime.now()
        insertionRows = []
        for input in series.data:
            insertionValueRow = {"isActual": None, "generatedTime": None, "isActual": None,"acquiredTime": None, "verifiedTime": None, "dataValue": None, "dataUnit": None, "dataSource": None, "dataLocation": None, "dataDatum": None, "latitude": None, "longitude": None}
            insertionValueRow["generatedTime"] = input.timeGenerated
            insertionValueRow["acquiredTime"] = now
            insertionValueRow["verifiedTime"] = input.timeVerified
            insertionValueRow["dataValue"] = input.dataValue
            insertionValueRow["isActual"] = False if series.description.dataSeries[0] == 'p' else True
            insertionValueRow["dataUnit"] = input.dataUnit
            insertionValueRow["dataSource"] = series.description.dataSource
            insertionValueRow["dataLocation"] = series.description.dataLocation
            insertionValueRow["dataSeries"] = series.description.dataSeries
            insertionValueRow["dataDatum"] = series.description.dataDatum
            insertionValueRow["latitude"] = input.latitude
            insertionValueRow["longitude"] = input.longitude
            insertionRows.append(insertionValueRow)

        with self.__get_engine().connect() as conn:
            cursor = conn.execute(insert(self.inputs)
                                    .on_conflict_do_nothing('inputs_AK00')
                                    .returning(self.inputs)
                                    .values(insertionRows)
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

        if(type(series.description).__name__ != 'SemaphoreSeriesDescription'): raise ValueError('Description should be type SemaphoreSeriesDescription')

        insertionValueRows = []
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
            
            insertionValueRows.append(insertionValueRow)

        with self.__get_engine().connect() as conn:
            cursor = conn.execute(insert(self.outputs)
                                  .on_conflict_do_nothing('outputs_AK00')
                                  .returning(self.outputs)
                                  .values(insertionValueRows)
                                  )
            result = cursor.fetchall()
            conn.commit()

        resultSeries = Series(series.description, True, series.timeDescription)
        resultSeries.data = self.__splice_output(result) #Turn tuple objects into actual objects
        return resultSeries
    


    def insert_ref_dataDatum(self, rows: list[dict]) -> list[tuple]:
        """This method inserts reference rows
            :param rows: A list of dictionaries. The dict can be found in NOTE 1
            :return Series - SQLALCHEMY tupleish rows
            NOTE:: {"code": None, "displayName": None, "notes": None}
        """
        with self.__get_engine().connect() as conn:
            cursor = conn.execute(insert(self.ref_dataDatum)
                                  .returning(self.ref_dataDatum)
                                  .values(rows)
                                  )
            result = cursor.fetchall()
            conn.commit()
            return result

    
    def insert_ref_dataLocation(self, rows: list[dict]) -> list[tuple]:
        """This method inserts reference rows
            :param rows: A list of dictionaries. The dict can be found in NOTE 1
            :return Series - SQLALCHEMY tupleish rows
            NOTE:: {"code": None, "displayName": None, "notes": None, "latitude": None, "longitude": None}
        """
        with self.__get_engine().connect() as conn:
            cursor = conn.execute(insert(self.ref_dataLocation)
                                  .returning(self.ref_dataLocation)
                                  .values(rows)
                                  )
            result = cursor.fetchall()
            conn.commit()
            return result
    

    def insert_ref_dataSeries(self, rows: list[dict]) -> list[tuple]:
        """This method inserts reference rows
            :param rows: A list of dictionaries. The dict can be found in NOTE 1
            :return Series - SQLALCHEMY tupleish rows
            NOTE:: {"code": None, "displayName": None, "notes": None}
        """
        
        with self.__get_engine().connect() as conn:
            cursor = conn.execute(insert(self.ref_dataSeries)
                                  .returning(self.ref_dataSeries)
                                  .values(rows)
                                  )
            result = cursor.fetchall()
            conn.commit()
            return result
    

    def insert_ref_dataSource(self, rows: list[dict]) -> list[tuple]:
        """This method inserts reference rows
            :param rows: A list of dictionaries. The dict can be found in NOTE 1
            :return Series - SQLALCHEMY tupleish rows
            NOTE:: {"code": None, "displayName": None, "notes": None}
        """
        
        with self.__get_engine().connect() as conn:
            cursor = conn.execute(insert(self.ref_dataSource)
                                  .returning(self.ref_dataSource)
                                  .values(rows)
                                  )
            result = cursor.fetchall()
            conn.commit()
        return result
    

    def insert_ref_dataUnit(self, rows: list[dict]) -> list[tuple]:
        """This method inserts reference rows
            :param rows: A list of dictionaries. The dict can be found in NOTE 1
            :return Series - SQLALCHEMY tupleish rows
            NOTE:: {"code": None, "displayName": None, "notes": None}
        """
        
        with self.__get_engine().connect() as conn:
            cursor = conn.execute(insert(self.ref_dataUnit)
                                  .returning(self.ref_dataUnit)
                                  .values(rows)
                                  )
            result = cursor.fetchall()
            conn.commit()
        return result
    
    def insert_data_mapping(self, rows: list[dict]) -> list[tuple]:
        """This method inserts reference rows
            :param rows: A list of dictionaries.
            :return Series - SQLALCHEMY tupleish rows
        """
        
        with self.__get_engine().connect() as conn:
            cursor = conn.execute(insert(self.dataLocation_dataSource_mapping)
                                  .returning(self.dataLocation_dataSource_mapping)
                                  .values(rows)
                                  )
            result = cursor.fetchall()
            conn.commit()
        return result

    def create_DB(self) -> None:
        """Creates the database with the tethered engine.
        Requires the engine to be created before it will create the DB.
        See: DBManager.create_engine()
        """

        self._metadata.create_all(self.__get_engine())
        self.__create_constraints()
     

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
        self._engine = sqlalchemy_create_engine(parmaString, echo=echo) #, pool_pre_ping=True

    
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

            Column("generatedTime", DateTime, nullable=True),
            Column("acquiredTime", DateTime, nullable=False),
            Column("verifiedTime", DateTime, nullable=False), 

            Column("dataValue", String(25), nullable=False), 

            Column("isActual", Boolean, nullable=False),

            Column("dataUnit", String(10), ForeignKey("ref_dataUnit.code"), nullable=False),  
            
            Column("dataSource", String(10), ForeignKey("ref_dataSource.code"), nullable=False),
            Column("dataLocation", String(25), ForeignKey("ref_dataLocation.code"), nullable=False), 
            Column("dataSeries", String(10), ForeignKey("ref_dataSeries.code"), nullable=False), 
            Column("dataDatum", String(10), ForeignKey("ref_dataDatum.code"),  nullable=True),
            
            Column("latitude", String(16), nullable=True),
            Column("longitude", String(16), nullable=True),
            # UniqueConstraint is made __create_constraints method
        )

        
        self.outputs = Table(
            "outputs",
            self._metadata,

            Column("id", Integer, autoincrement=True, primary_key=True),
            
            Column("timeGenerated", DateTime, nullable=False),

            Column("leadTime", Interval, nullable=False),

            Column("modelName", String(25), nullable=False), 
            Column("modelVersion", String(10), nullable=False),
            Column("dataValue", String(25), nullable=False), 
            Column("dataUnit", String(10), ForeignKey("ref_dataUnit.code"), nullable=False), 
            Column("dataLocation", String(25), ForeignKey("ref_dataLocation.code"), nullable=False),   
            Column("dataSeries", String(10), ForeignKey("ref_dataSeries.code"), nullable=False),         
            Column("dataDatum", String(10), ForeignKey("ref_dataDatum.code"), nullable=True),

            # UniqueConstraint is made __create_constraints method
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

            UniqueConstraint("dataLocationCode", "dataSourceCode", "dataSourceLocationCode", "priorityOrder", name='dataLocation_dataSource_mapping_AK00'),
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
            
            Column("code", String(25), nullable=False, unique=True),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),
            Column("latitude", String(16), nullable=False),
            Column("longitude", String(16), nullable=False),

        )

        self.ref_dataSource = Table(
            "ref_dataSource",
            self._metadata,

            Column("id", Integer, autoincrement=True, primary_key=True),
            
            Column("code", String(10), nullable=False, unique=True),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

        )

        self.ref_dataSeries = Table(
            "ref_dataSeries",
            self._metadata,

            Column("id", Integer, autoincrement=True, primary_key=True),
            
            Column("code", String(10), nullable=False, unique=True),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),
        )

        self.ref_dataUnit = Table(
            "ref_dataUnit",
            self._metadata,

            Column("id", Integer, autoincrement=True, primary_key=True),
            
            Column("code", String(10), nullable=False, unique=True),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),
        )

        self.ref_dataDatum = Table(
            "ref_dataDatum",
            self._metadata,

            Column("id", Integer, autoincrement=True, primary_key=True),
            
            Column("code", String(10), nullable=False, unique=True),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),
        )

    def __create_constraints(self) -> None:
        """Creates the special unique constraints for input and output
        """
        inputs_AK00_stmt = text("""ALTER TABLE inputs ADD CONSTRAINT "inputs_AK00" UNIQUE NULLS NOT DISTINCT ("isActual", "generatedTime", "verifiedTime", "dataUnit", "dataSource", "dataLocation", "dataSeries", "dataDatum", "latitude", "longitude")""")
        outputs_AK00_stmt = text("""ALTER TABLE outputs ADD CONSTRAINT "outputs_AK00" UNIQUE NULLS NOT DISTINCT ("timeGenerated", "leadTime", "modelName", "modelVersion", "dataLocation", "dataSeries", "dataDatum")""")

        with self.__get_engine().connect() as conn:
            conn.execute(inputs_AK00_stmt)
            conn.execute(outputs_AK00_stmt)
            conn.commit()

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
                row[verifiedTimeIndex],
                row[generatedTimeIndex],
                row[longitudeIndex],
                row[latitudeIndex]
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

    def insert_lat_lon_test(self, code: str, displayName: str, notes: str, latitude: str, longitude: str):
        """This method inserts lat and lon information
        """
        #Construct DB row to insert
        insertionValueRow = {"code": code, "displayName": displayName, "notes": notes, "latitude": latitude, "longitude": longitude}
        
        with self.__get_engine().connect() as conn:
            conn.execute(insert(self.ref_dataLocation)
                        .values(insertionValueRow))
            conn.commit()

    def insert_external_location_code(self, dataLocationCode: str, dataSourceCode: str, dataSourceLocationCode: str, priorityOrder: int):
        """This method inserts external location code information
        """
        #Construct DB row to insert
        insertionValueRow = {"dataLocationCode": dataLocationCode, "dataSourceCode": dataSourceCode, "dataSourceLocationCode": dataSourceLocationCode, "priorityOrder": priorityOrder}
        
        with self.__get_engine().connect() as conn:
            conn.execute(insert(self.dataLocation_dataSource_mapping)
                        .values(insertionValueRow))
            conn.commit()