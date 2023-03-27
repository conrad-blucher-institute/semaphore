      
# -*- coding: utf-8 -*-
#DBInterface.py
#----------------------------------
# Created By : Matthew Kastl
# Created Date: 3/26/2023
# version 1.0
#----------------------------------
""" This script defines a class that hold the Semaphore DB schema. It also has funtions to 
    generate engines, and to create and drop the database.
 """ 
#----------------------------------
# 
#
#Imports
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, DateTime, Float, MetaData, UniqueConstraint, Engine, ForeignKey

class DBInterface():
    
    def __init__(self) -> None:
        """Constructor generates an a db schema. Automatically creates the 
        metadata object holding the defined scema.
        """
        self.__create_schema()

    def create_DB(self) -> None:
        """Creates the database with the tethered engine.
        Requires the engine to be created before it will create the DB.
        See: DBInterface.create_engine()
        """
        try:
            self._metadata_obj.create_all(self._engine)
        except AttributeError:
            raise Exception("An engine was requestied from DBInterface, but no engine has been created.")
        

    def drop_DB(self) -> None:
        """Drops the database with the tethered engine.
        Requires the engine to be created before it will drop the DB.
        See: DBInterface.create_engine()
        """
        try:
            self._metadata_obj.drop_all(self._engine)
        except AttributeError:
            raise Exception("An engine was requestied from DBInterface, but no engine has been created.")

    def create_engine(self, parmaString: str, echo: str ) -> None: #"sqlite+pysqlite:///:memory:"
        """Creates an engine object and tethers it to this interface class as an atribute

        Parameters:
            permaString: str - An sqlalchemy string that defines the location the engine should point to: (e.g. "sqlite+pysqlite:///:memory:")
            echo: str - Weather or not the engine should echo to stdout
        """
        self._engine = create_engine(parmaString, echo=echo)
    
    def get_engine(self) -> Engine:
        """Fetches the engine atribute. Requires the engine atribute to be created.
        See: DBInterface.create_engine()
        """
        try:
            return self._engine
        except AttributeError:
            raise Exception("An engine was requestied from DBInterface, but no engine has been created.")
        
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



