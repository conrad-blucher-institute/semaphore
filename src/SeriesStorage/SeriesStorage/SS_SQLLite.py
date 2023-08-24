# -*- coding: utf-8 -*-
#SeriesStorage.py
#-------------------------------
# Created By : Matthew Kastl
# Created Date: 3/26/2023
# version 4.0
#-------------------------------
""" This script defines a class that hold the Semaphore DB schema. It also has funtions to 
    manage the DB and interact with the db.
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
from dotenv import load_dotenv
from os import getenv


from src.SeriesStorage.ISeriesStorage import ISeriesStorage

from SeriesProvider.DataClasses import *

class SS_SQLLite(ISeriesStorage):

    #############################################################################################
    ################################################################################## Interface methods
    #############################################################################################
    def select_actuals(self, seriesDescription) -> Series:
        return self.s_data_point_selection(seriesDescription)
    

    def select_prediction(self, seriesDescription) -> Series:
        return self.s_prediction_selection(seriesDescription)
    

    def find_external_location_code(self, sourceCode, location, priorityOrder: int = 0) -> str:
        return self.s_locationCode_dataSourceLocationCode_mapping_select(sourceCode, location, priorityOrder)


    def insert_actuals(self, Series) -> Series:
        return self.s_data_point_insert(Series)


    def insert_predictions(self, Series) -> Series:
        return self.s_prediction_insert(Series)
    

    def insert_output(self, Series) -> Series:
        return self.s_prediction_output_insert(Series)

    #############################################################################################
    ################################################################################## Class start
    #############################################################################################


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


    def create_engine(self, parmaString: str, echo: bool ) -> None: #"sqlite+pysqlite:///:memory:"
        """Creates an engine object and tethers it to this interface class as an atribute

        Parameters:
            permaString: str - An sqlalchemy string that defines the location the engine should point to: (e.g. "sqlite+pysqlite:///:memory:")
            echo: str - Weather or not the engine should echo to stdout
        """
        self._engine = sqlalchemy_create_engine(parmaString, echo=echo)

    
    def get_engine(self) -> Engine:
        """Fetches the engine atribute. Requires the engine atribute to be created.
        See: DBManager.create_engine()
        """

        if not hasattr(self, "_engine"):
            raise Exception("An engine was requestied from DBManager, but no engine has been created. See DBManager.create_engine()")
        else:
            return self._engine


    def get_metadata(self) -> MetaData:
        """Fetches metadata that hold the DB schema
        """
        return self._metadata_obj


    def __create_schema(self) -> None:
        """Builds the db schema in the metadata.
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
            Column("interval", Integer, nullable=False),
        
            Column("dataSourceCode", String(10), ForeignKey("s_ref_data_source.code"), nullable=False), #CBI specific ID for the Location
            Column("sLocationCode", String(25), ForeignKey("s_ref_slocation.code"), nullable=False),    #the code for the source from which the value was obtained e.g, NOAA
            Column("seriesCode", String(10), ForeignKey("s_ref_series.code"), nullable=False),          #The code fot the type of measurment or prediction e.g, wdir
            Column("datumCode", String(10), ForeignKey("s_ref_datum.code"), nullable=True),             #the datum(e.g., water-level, harmonic)
            Column("latitude", String(16), nullable=True),
            Column("longitude", String(16), nullable=True),

            UniqueConstraint("timeActualized", "dataValue", "unitsCode", "interval", "dataSourceCode", "sLocationCode", "seriesCode"),
        )

        self.s_prediction = Table(
            "s_prediction",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),

            Column("timeGenerated", DateTime, nullable=False),  #The time at which the prediction was made
            Column("leadTime", Float, nullable=False),          #the amount of hours till the predicted even occurs

            Column("dataValue", String(20), nullable=False),    #the actual value
            Column("unitsCode", String(10), ForeignKey("s_ref_units.code"), nullable=False), #the units the data point is stored in
            Column("interval", Integer, nullable=False),

            Column("resultCode", String(10), nullable=True), #some value that discribes the quality of the pridiction

            Column("dataSourceCode", String(10), ForeignKey("s_ref_data_source.code"), nullable=False),     #CBI specific ID for the Location
            Column("sLocationCode", String(25), ForeignKey("s_ref_slocation.code"), nullable=False),        #the code for the source from which the value was obtained e.g, NOAA
            Column("seriesCode", String(10), ForeignKey("s_ref_series.code"), nullable=False),              #The code fot the type of measurment or prediction e.g, wdir
            Column("datumCode", String(10), ForeignKey("s_ref_datum.code"), nullable=True),                 #the datum(e.g., water-level, harmonic)
            Column("latitude", String(16), nullable=True),
            Column("longitude", String(16), nullable=True),

            UniqueConstraint("timeGenerated", "leadTime", "dataValue", "unitsCode", "interval", "dataSourceCode", "sLocationCode", "seriesCode"),
        )

        
        self.s_prediction_output = Table(
            "s_prediction_output",
            self._metadata_obj,

            Column("id", Integer, autoincrement=True, primary_key=True),
            Column("timeAquired", DateTime, nullable=False),    #When the data was inserted by us

            Column("timeGenerated", DateTime, nullable=False),  #The time at which the prediction was made
            Column("leadTime", Float, nullable=False),          #the amount of hours till the predicted even occurs

            Column("ModelName", String(25), nullable=False),  #The name of the Ai that generated this
            Column("ModelVersion", String(10), nullable=False),  #The name of the Ai that generated this

            Column("dataValue", String(20), nullable=False),    #the actual value
            Column("unitsCode", String(10), ForeignKey("s_ref_units.code"), nullable=False), #the units the data point is stored in

            Column("sLocationCode", String(25), ForeignKey("s_ref_slocation.code"), nullable=False),        #the code for the source from which the value was obtained e.g, NOAA
            Column("seriesCode", String(10), ForeignKey("s_ref_series.code"), nullable=False),              #The code fot the type of measurment or prediction e.g, wdir
            Column("datumCode", String(10), ForeignKey("s_ref_datum.code"), nullable=True),                 #the datum(e.g., water-level, harmonic)

            UniqueConstraint("timeGenerated", "leadTime", "ModelName", "ModelVersion"),
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
            Column("latitude", String(16), nullable=False),
            Column("longitude", String(16), nullable=False),

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

    def __dbSelection(self, stmt: Select) -> CursorResult:
        """Runs a slection statment 
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
        resultSeries.bind_data(self.__splice_actual_results(result)) #Turn tuple objects into prediction objects
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
            .where((table.c.timeGenerated + table.c.leadTime) >= seriesDescription.fromDateTime)
            .where((table.c.timeGenerated + table.c.leadTime) <= seriesDescription.toDateTime)
        )

        result = self.__dbSelection(stmt).fetchall()
        resultSeries = Series(seriesDescription, True)
        resultSeries.bind_data(self.__splice_prediction_results(result)) #Turn tuple objects into prediction objects
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

        #Consturct DB row to insert
        now = datetime.now()
        insertionRows = []
        for actual in series.get_data():
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
        resultSeries.bind_data(self.__splice_actual_results(result)) #Turn tuple objects into actual objects
        return resultSeries
    


    def s_prediction_insert(self, series: Series) -> Series:
        """Inserts a series into s_prediction
        Parameter:
            Series - The data to insert.
        Returns:
            Series - The data actually inserted.
        """

        insertionRows = []
        for prediction in series.get_data():
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
        resultSeries.bind_data(self.__splice_prediction_results(result)) #Turn tuple objects into prediction objects
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
        for prediction in series.get_data():

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
                                  )
            result = cursor.fetchall()
            conn.commit()

        resultSeries = Series(series.description, True)
        resultSeries.description = series.description
        resultSeries.bind_data(self.__splice_output_table_results(result)) #Turn tuple objects into prediction objects
        return resultSeries
    

    def s_locationCode_dataSourceLocationCode_mapping_insert(self, values: dict | list[tuple]) -> list[tuple]:
        """Inserts a row or batch into s_locationCode_dataSourceLocationCode_mapping

        Dictionary reference: {"dataSourceCode", "sLocationCode", "dataSourceLocationCode", "priorityOrder"}

        Parameters:
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.

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
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.

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
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.

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
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.

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
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.

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
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.

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
            values: dict | list[tuple] - THe dictionary containing the inssersion valuess (see dictionary reference above). Can either be one dictionary or a list of dictionaries.

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
    

