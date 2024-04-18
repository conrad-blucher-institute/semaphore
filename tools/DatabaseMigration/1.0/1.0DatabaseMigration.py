# -*- coding: utf-8 -*-
#1.0DatabaseMigration.py
#----------------------------------
# Created By: Savannah Stephenson
# Created Date: 04/16/2024
# Version 1.0
#----------------------------------
"""This is a database migration script that will initialize version
    1.0 of the database (without using the ORM)
 """ 
#----------------------------------
# 
#
#Imports
from IDatabaseMigration import IDatabaseMigration
from sqlalchemy import create_engine 
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, UniqueConstraint, Engine, ForeignKey, Boolean, Interval, text
from sqlalchemy.dialects.postgresql import insert
from os import getenv
from dotenv import load_dotenv
import csvs

load_dotenv()

def main():

    # Load database location string
    DB_LOCATION_STRING = os.getenv('DB_LOCATION_STRING')

    # Create the database engine
    engine = create_engine(DB_LOCATION_STRING)

    #Perform Update (there is no rollback method for this class)
    if (Migration_1_0.update(engine)):
        #some kind of log statement? 
        pass
    

if __name__ == "__main__":
    main()

#Implementing methods of interface
class Migration_1_0(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        #setting engine
        self.__engine = databaseEngine

        #create version 1.0 metadata object and 
        self.__create_schema()

        #pass metadata object to create version 1.0 schema
        self._metadata.create_all(databaseEngine)

        #specifying the alternate keys
        inputs_AK00_stmt = text("""ALTER TABLE inputs ADD CONSTRAINT "inputs_AK00" UNIQUE NULLS NOT DISTINCT ("isActual", "generatedTime", "verifiedTime", "dataUnit", "dataSource", "dataLocation", "dataSeries", "dataDatum", "latitude", "longitude")""")
        outputs_AK00_stmt = text("""ALTER TABLE outputs ADD CONSTRAINT "outputs_AK00" UNIQUE NULLS NOT DISTINCT ("timeGenerated", "leadTime", "modelName", "modelVersion", "dataLocation", "dataSeries", "dataDatum")""")


        #with schema, fill in alternate keys and version 1.0 rows
        with databaseEngine.connect() as connection:
            #create the alternate key constraints
            connection.execute(inputs_AK00_stmt)
            connection.execute(outputs_AK00_stmt)
            connection.commit()

        #insert the rows from first csv files
        self.insert_ref_dataDatum(readInitCSV('dataDatum.csv'))
        self.insert_ref_dataLocation(readInitCSV('dataLocation.csv'))
        self.insert_ref_dataSeries(readInitCSV('dataSeries.csv'))
        self.insert_ref_dataSource(readInitCSV('dataSource.csv'))
        self.insert_ref_dataUnit(readInitCSV('dataUnit.csv'))
        self.insert_data_mapping(readInitCSV('dataMapping.csv'))


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
            Column("dataSeries", String(25), ForeignKey("ref_dataSeries.code"), nullable=False), 
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
            Column("dataSeries", String(25), ForeignKey("ref_dataSeries.code"), nullable=False),         
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
            
            Column("code", String(25), nullable=False, unique=True),
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

    def readInitCSV(csvFileName: str) -> list:
        """This function reads in a CSV file with the data needed for the initialization 
            of the database
            :param csvFileName: str - CSV file name
            
            :return: list of dictionaries
        """
        csvFilePath = f'./1.0/init_data/{csvFileName}'
        dictionaryList = []
        with open(csvFilePath, mode = 'r') as infile:
            csvDict = csv.DictReader(infile)
            for dictionary in csvDict:
                dictionaryList.append(dictionary)

        return dictionaryList


    def insert_ref_dataDatum(self, rows: list[dict]) -> list[tuple]:
        """This method inserts reference rows
            :param rows: A list of dictionaries. The dict can be found in NOTE 1
            :return Series - SQLALCHEMY tupleish rows
            NOTE:: {"code": None, "displayName": None, "notes": None}
        """
        with self.__engine.connect() as conn:
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
        with self.__engine.connect() as conn:
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
        
        with self.__engine.connect() as conn:
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
        
        with self.__engine.connect() as conn:
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
        
        with self.__engine.connect() as conn:
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
        
        with self.__engine.connect() as conn:
            cursor = conn.execute(insert(self.dataLocation_dataSource_mapping)
                                  .returning(self.dataLocation_dataSource_mapping)
                                  .values(rows)
                                  )
            result = cursor.fetchall()
            conn.commit()
        return result