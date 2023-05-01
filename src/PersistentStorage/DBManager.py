      
# -*- coding: utf-8 -*-
#DBInterface.py
#----------------------------------
# Created By : Matthew Kastl
# Created Date: 3/26/2023
# version 2.0
#----------------------------------
""" This script defines a class that hold the Semaphore DB schema. It also has funtions to 
    manage the DB and interact with the db.
 """ 
#----------------------------------
# 
#
#Imports
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, DateTime, Float, MetaData, UniqueConstraint, Engine, ForeignKey, insert, CursorResult, Select, select
from dotenv import load_dotenv
from os import getenv
from utility import log

from DataManagement.DataClasses import *

class DBManager():
    
    def __init__(self) -> None:
        """Constructor generates an a db schema. Automatically creates the 
        metadata object holding the defined scema.
        """
        self.__create_schema()
        load_dotenv()
        self.create_engine(getenv('DB_LOCATION_STRING'), False)

    #############################################################################################
    ################################################################################## DB Managment Methods
    #############################################################################################

    def create_DB(self) -> None:
        """Creates the database with the tethered engine.
        Requires the engine to be created before it will create the DB.
        See: DBManager.create_engine()
        """

        self._metadata_obj.create_all(self.get_engine())
     

    def drop_DB(self) -> None:
        """Drops the database with the tethered engine.
        Requires the engine to be created before it will drop the DB.
        See: DBManager.create_engine()
        """

        self._metadata_obj.drop_all(self.get_engine())


    def create_engine(self, parmaString: str, echo: str ) -> None: #"sqlite+pysqlite:///:memory:"
        """Creates an engine object and tethers it to this interface class as an atribute

        Parameters:
            permaString: str - An sqlalchemy string that defines the location the engine should point to: (e.g. "sqlite+pysqlite:///:memory:")
            echo: str - Weather or not the engine should echo to stdout
        """
        self._engine = create_engine(parmaString, echo=echo)

    
    def get_engine(self) -> Engine:
        """Fetches the engine atribute. Requires the engine atribute to be created.
        See: DBManager.create_engine()
        """

        if not hasattr(self, "_engine"):
            raise Exception("An engine was requestied from DBManager, but no engine has been created. See DBManager.create_engine()")
        else:
            return self._engine


    def get_metadata(self) -> MetaData:
        """Fetches interface metadata that hold the DB schema
        """
        return self._metadata_obj


    def __create_schema(self) -> None:
        """This private meta builds the db schema in the metadata.
        """

        self._metadata_obj = MetaData()
        
        #this table stores the actual data values as retrieved or received 
        self.s_data_point = Table(
            "s_data_point",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),

            Column("timeActualized", DateTime, nullable=False), #timestamp for the value in UTC GMT
            Column("timeAquired", DateTime, nullable=False),    #When the data was inserted by us

            Column("dataValue", String(20), nullable=False),    #Actual value for data points
            Column("unitsCode", String(10), ForeignKey("s_ref_units.code"), nullable=False), #unit the value is stored in

            Column("dataSourceCode", String(10), ForeignKey("s_ref_data_source.code"), nullable=False), #CBI specific ID for the Location
            Column("sLocationCode", String(25), ForeignKey("s_ref_slocation.code"), nullable=False),    #the code for the source from which the value was obtained e.g, NOAA
            Column("seriesCode", String(10), ForeignKey("s_ref_series.code"), nullable=False),          #The code fot the type of measurment or prediction e.g, wdir
            Column("datumCode", String(10), ForeignKey("s_ref_datum.code"), nullable=True),             #the datum(e.g., water-level, harmonic)
            Column("latitude", String(16), nullable=True),
            Column("longitude", String(16), nullable=True),

            UniqueConstraint("timeActualized", "timeAquired", "dataValue", "unitsCode", "dataSourceCode", "sLocationCode", "seriesCode"),
        )

        self.s_prediction = Table(
            "s_prediction",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),

            Column("timeGenerated", DateTime, nullable=False),  #The time at which the prediction was made
            Column("leadTime", Float, nullable=False),          #the amount of hours till the predicted even occurs

            Column("dataValue", String(20), nullable=False),    #the actual value
            Column("unitsCode", String(10), ForeignKey("s_ref_units.code"), nullable=False), #the units the data point is stored in

            Column("resultCode", String(10), nullable=True), #some value that discribes the quality of the pridiction
            Column("resultCodeUnit", String(10), ForeignKey("s_ref_resultCodeUnits.code"), nullable=True), #how that quality is stored

            Column("dataSourceCode", String(10), ForeignKey("s_ref_data_source.code"), nullable=False),     #CBI specific ID for the Location
            Column("sLocationCode", String(25), ForeignKey("s_ref_slocation.code"), nullable=False),        #the code for the source from which the value was obtained e.g, NOAA
            Column("seriesCode", String(10), ForeignKey("s_ref_series.code"), nullable=False),              #The code fot the type of measurment or prediction e.g, wdir
            Column("datumCode", String(10), ForeignKey("s_ref_datum.code"), nullable=True),                 #the datum(e.g., water-level, harmonic)
            Column("latitude", String(16), nullable=False),
            Column("longitude", String(16), nullable=False),

            UniqueConstraint("timeGenerated", "leadTime", "dataValue", "unitsCode", "dataSourceCode", "sLocationCode", "seriesCode"),
        )

        #This table maps CBI location codes to location codes used by datasorces
        self.s_locationCode_dataSourceLocationCode_mapping = Table(
            "s_locationCode_dataSourceLocationCode_mapping",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),
            
            Column("sLocationCode", String(25), ForeignKey("s_ref_slocation.code"), nullable=False),    #Local term
            Column("dataSourceCode", String(10), ForeignKey("s_ref_data_source.code"), nullable=False), #Data source
            Column("dataSourceLocationCode", String(255), nullable=False),                              #Forien term
            Column("priorityOrder", Integer, nullable=False),                                           #priority of use

            UniqueConstraint("sLocationCode", "dataSourceCode", "dataSourceLocationCode", "priorityOrder"),
        )


        #The rest of these tables are reference tables for values stored in the tables above. They all contain
        # ID - aoutincamented id
        # code - that mapped code
        # display name - a non compressed pretty name
        # notes - more information about that item
        self.s_ref_slocation = Table(
            "s_ref_slocation",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),
            Column("code", String(25), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )

        self.s_ref_data_source = Table(
            "s_ref_data_source",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),
            Column("code", String(10), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )

        self.s_ref_series = Table(
            "s_ref_series",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),
            Column("code", String(10), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )

        self.s_ref_units = Table(
            "s_ref_units",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),
            Column("code", String(10), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )

        self.s_ref_datum = Table(
            "s_ref_datum",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),
            Column("code", String(10), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )
        
        self.s_ref_resultCodeUnits = Table(
            "s_ref_resultCodeUnits",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),
            Column("code", String(10), nullable=False),
            Column("displayName", String(30), nullable=False),
            Column("notes", String(250), nullable=True),

            UniqueConstraint("code", "displayName"),
        )

    #############################################################################################
    ################################################################################## DB Interaction private methods
    #############################################################################################

    def __cursorToList(self, cursor: CursorResult) -> list[tuple]:
        """Converts a SQLAlchemy cursor to a generic list[tuple] obj"""
        return [tuple(row) for row in cursor]
        

    def __dbSelection(self, stmt: Select) -> CursorResult:
        """Runs a slection statment 
        ------
        ------
        Parameters:

            stmt: SQLAlchemy Select - The statement to run
        ------
        Returns:
            SQLAlchemy CursorResult
        """

        with self.get_engine().connect() as conn:
            result = conn.execute(stmt)

        return result

    #############################################################################################
    ################################################################################## Public selection methods
    #############################################################################################
    
    def s_data_point_selection(self, sourceCode: str, seriesCode: str, locationCode: str, startTime: datetime, endTime: datetime, datumCode: str = '') -> list[tuple]:
        """Selects from the data point table.
        -------
        Returns None if DNE
        """
        table = self.s_data_point
        stmt = (select(table)
            .where(table.c.dataSourceCode == sourceCode)
            .where(table.c.sLocationCode == locationCode)
            .where(table.c.seriesCode == seriesCode)
            .where(table.c.datumCode == datumCode)
            .where(table.c.timeActualized >= startTime)
            .where(table.c.timeActualized <= endTime)
        )
        
        return self.__cursorToList(self.__dbSelection(stmt))
    
    def s_prediction_selection(self, sourceCode: str, seriesCode: str, locationCode: str, startTime: datetime, endTime: datetime, datumCode: str = '') -> list[tuple]:
        """Selects from the prediction table.
        -------
        Returns None if DNE
        """
        table = self.s_prediction
        stmt = (select(table)
            .where(table.c.dataSourceCode == sourceCode)
            .where(table.c.sLocationCode == locationCode)
            .where(table.c.seriesCode == seriesCode)
            .where(table.c.datumCode == datumCode)
            .where((table.c.timeGenerated + table.c.leadTime) >= startTime)
            .where((table.c.timeGenerated + table.c.leadTime) <= endTime)
        )

        return self.__cursorToList(self.__dbSelection(stmt))
        

    def s_locationCode_dataSourceLocationCode_mapping_select(self, sourceCode: str, location: str, priorityOrder: int = 0) -> list[tuple]:
        """Selects a a dataSourceLocationCode given a datasource and a location. 
        -------
        Returns None if DNE
        """
        table = self.s_locationCode_dataSourceLocationCode_mapping
        stmt = (select(table)
                .where(table.c.dataSourceCode == sourceCode)
                .where(table.c.sLocationCode == location)
                .where(table.c.priorityOrder == priorityOrder)
                )

        return self.__cursorToList(self.__dbSelection(stmt))

    #############################################################################################
    ################################################################################## Purblic insertion Methods
    #############################################################################################

    def s_data_point_insert(self, values: dict | list[tuple]) -> CursorResult:
        """Inserts a row or batch into s_data_point
        ------
        Dictionary reference: {"timeActualized", "timeAquired", "dataValue", "unitsCode", "dataSourceCode", "sLocationCode", "seriesCode", (OP)"datumCode", (OP)"latitude", (OP)"longitude"}
        ------
        Parameters:
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
        ------
        Returns:
            SQLAlchemy CursorResult
        """

        with self.get_engine().connect() as conn:
            result = conn.execute(insert(self.s_data_point)
                                  .returning(self.s_data_point), 
                                  values
                                  )
            conn.commit()

        return self.__cursorToList(result)


    def s_prediction_insert(self, values: dict | list[tuple]) -> CursorResult:
        """Inserts a row or batch into s_predictions
        ------
        Dictionary reference: {"timeGenerated", "leadTime", "dataValue", "unitsCode", (OP)"resultCode", (OP)"resultCodeUnit", "dataSourceCode", "sLocationCode", "seriesCode", (OP)"datumCode", (OP)"latitude", (OP)"longitude"}
        ------
        Parameters:
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
        ------
        Returns:
            SQLAlchemy CursorResult
        """

        with self.get_engine().connect() as conn:
            result = conn.execute(insert(self.s_prediction)
                                  .returning(self.s_prediction), 
                                  values
                                  )
            conn.commit()

        return self.__cursorToList(result)


    def s_locationCode_dataSourceLocationCode_mapping_insert(self, values: dict | list[tuple]) -> CursorResult:
        """Inserts a row or batch into s_locationCode_dataSourceLocationCode_mapping
        ------
        Dictionary reference: {"dataSourceCode", "sLocationCode", "dataSourceLocationCode", "priorityOrder"}
        ------
        Parameters:
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
        ------
        Returns:
            SQLAlchemy CursorResult
        """

        with self.get_engine().connect() as conn:
            result = conn.execute(insert(self.s_locationCode_dataSourceLocationCode_mapping)
                                  .returning(self.s_locationCode_dataSourceLocationCode_mapping), 
                                  values
                                  )
            conn.commit()

        return self.__cursorToList(result)


    def s_ref_slocation_insert(self, values: dict | list[tuple]) -> CursorResult:
        """Inserts a row or batch into s_ref_slocation
        ------
        Dictionary reference: {"code", "displayName", (OP)"notes"}
        ------
        Parameters:
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
        ------
        Returns:
            SQLAlchemy CursorResult
        """
        
        with self.get_engine().connect() as conn:
            result = conn.execute(insert(self.s_ref_slocation)
                                  .returning(self.s_ref_slocation), 
                                  values
                                  )
            conn.commit()

        return self.__cursorToList(result)


    def s_ref_data_source_insert(self, values: dict | list[tuple]) -> CursorResult:
        """Inserts a row or batch into s_ref_data_source
        ------
        Dictionary reference: {"code", "displayName", (OP)"notes"}
        ------
        Parameters:
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
        ------
        Returns:
            SQLAlchemy CursorResult
        """

        with self.get_engine().connect() as conn:
            result = conn.execute(insert(self.s_ref_data_source)
                                  .returning(self.s_ref_data_source), 
                                  values
                                  )
            conn.commit()

        return self.__cursorToList(result)


    def s_ref_series_insert(self, values: dict | list[tuple]) -> CursorResult:
        """Inserts a row or batch into s_ref_series
        ------
        Dictionary reference: {"code", "displayName", (OP)"notes"}
        ------
        Parameters:
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
        ------
        Returns:
            SQLAlchemy CursorResult
        """

        with self.get_engine().connect() as conn:
            result = conn.execute(insert(self.s_ref_series)
                                  .returning(self.s_ref_series), 
                                  values
                                  )
            conn.commit()

        return self.__cursorToList(result)


    def s_ref_units_insert(self, values: dict | list[tuple]) -> CursorResult:
        """Inserts a row or batch into s_ref_units
        ------
        Dictionary reference: {"code", "displayName", (OP)"notes"}
        ------
        Parameters:
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
        ------
        Returns:
            SQLAlchemy CursorResult
        """

        with self.get_engine().connect() as conn:
            result = conn.execute(insert(self.s_ref_units)
                                  .returning(self.s_ref_units), 
                                  values
                                  )
            conn.commit()

        return self.__cursorToList(result)


    def s_ref_datum_insert(self, values: dict | list[tuple]) -> CursorResult:
        """Inserts a row or batch into s_ref_datum
        ------
        Dictionary reference: {"code", "displayName", (OP)"notes"}
        ------
        Parameters:
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
        ------
        Returns:
            SQLAlchemy CursorResult
        """

        with self.get_engine().connect() as conn:
            result = conn.execute(insert(self.s_ref_datum)
                                  .returning(self.s_ref_datum), 
                                  values
                                  )
            conn.commit()

        return self.__cursorToList(result)


    def s_ref_resultCode_insert(self, values: dict | list[tuple]) -> CursorResult:
        """Inserts a row or batch into s_ref_resultCode
        ------
        Dictionary reference: {"code", "displayName", (OP)"notes"}
        ------
        Parameters:
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.
        ------
        Returns:
            SQLAlchemy CursorResult
        """

        with self.get_engine().connect() as conn:
            result = conn.execute(insert(self.s_ref_resultCode)
                                  .returning(self.s_ref_resultCode), 
                                  values
                                  )
            conn.commit()

        return self.__cursorToList(result)