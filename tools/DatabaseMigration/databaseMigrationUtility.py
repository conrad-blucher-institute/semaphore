# -*- coding: utf-8 -*-
#databaseMigrationUtility.py
#----------------------------------
# Created By: Ops Team
# Created Date: 5/20/2024
# Version: 1.0
#----------------------------------
""" This file houses basic utility functions for database migration operations. 
 """ 
#----------------------------------
# 
#
#Imports
from enum import Enum
from sqlalchemy import Engine, MetaData, delete

class KeywordType(Enum):
    """A class to enumerate the different keyword types. 
    """
    DATA_UNIT = 0
    DATA_SOURCE = 1
    DATA_LOCATION = 2
    DATA_SERIES = 3
    DATA_DATUM = 4

KEYWORD_DEPENDENT_TABLES = {
    KeywordType.DATA_UNIT:{
        "column_name": "dataUnit",
        "primaries":["inputs", "outputs"],
        "reference": "ref_dataUnit"
    },
    KeywordType.DATA_SOURCE:{
        "column_name": "dataSource",
        "primaries":["inputs", "dataLocation_dataSource_mapping"],
        "reference": "ref_dataSource"
    },
    KeywordType.DATA_LOCATION:{
        "column_name": "dataLocation",
        "primaries":["inputs", "outputs", "dataLocation_dataSource_mapping"],
        "reference": "ref_dataLocation"
    },
    KeywordType.DATA_SERIES:{
        "column_name": "dataSeries",
        "primaries":["inputs", "outputs"],
        "reference": "ref_dataSeries"
    },
    KeywordType.DATA_DATUM:{
        "column_name": "dataDatum",
        "primaries":["inputs", "outputs"],
        "reference": "ref_dataDatum"
    }
}

class DatabaseDeletionHelper():
    """ A class with helper functions to delete data from the database. 
        Intended to be used in data migration rollback methods. 
        :param dataEngine - Engine - The database we are working with.
    """

    def __init__(self, databaseEngine: Engine):
        self.__engine = databaseEngine
        metadata = MetaData()
        metadata.reflect(bind=databaseEngine)
        self.__metadata = metadata

    def __delete_dependant_rows(self, keyword: str, tableName: str, columnName: str):
        """ A function to delete dependant rows according to the passed keyword, tableName, and columnName. 
            :param: keyword - str - The foreign key we want to delete.
            :param: tableName - str - The name of the table we want to delete from.
            :param: columnName - str - The name of the colum that the keyword is in. 
        """
        # In dataLocation_dataSource_mapping table the column names are different from the columns in the other tables
        processedColumnName = columnName if tableName != "dataLocation_dataSource_mapping" else f'{columnName}Code'
        
        # Connect to the database and delete 
        with self.__engine.connect() as conn:

            # Reflect the table we want to delete from
            table = self.__metadata.tables[tableName]
            
            # Now delete the dependent data rows
            delete_stmt = delete(table).where(table.c[processedColumnName] == keyword).returning(table)
            cursor = conn.execute(delete_stmt)
            result = cursor.fetchall()  
            conn.commit()

        # Write deleted data to file
        self.__deletion_data_dump(result)

    def __delete_reference_row(self, keyword: str, tableName: str):
        """ A function to delete reference rows according to the passed keyword and tableName. 
            :param: keyword - str - The foreign key we want to delete.
            :param: tableName - str - The name of the table we want to delete from.
        """
        # Connect to the database and delete reference row
        with self.__engine.connect() as conn:

            # Reflect the table we want to delete from
            table = self.__metadata.tables[tableName]
            
            # Now delete the reference data rows
            delete_stmt = delete(table).where(table.c.code == keyword).returning(table)
            cursor = conn.execute(delete_stmt)
            result = cursor.fetchall()  
            conn.commit()

        # Write deleted data to file
        self.__deletion_data_dump(result)

    def __deletion_data_dump(self, result: any): 
        """ A function to write deleted database information to a file for safekeeping. 
            :param: result - any - The information we want to keep.
        """
        with open("deletion_dump.txt", "a") as file:
            for line in result:
                file.write(str(line)+"\n")

    def deep_delete_keyword(self, keyword: str, keywordType: KeywordType):
        """ A function to delete foreign keys from the database that 
            potentially have dependent data in the primary tables. 
            :param: keyword - str - The foreign key we want to delete.
            :param: keywordType - KeywordType - The enumerator of the type of foreign key that the keyword is.
        """

        # Depending on keyword type get dictionary information
        keywordInfo = KEYWORD_DEPENDENT_TABLES[keywordType]

        # Delete keyword from dependent tables
        for tableName in keywordInfo["primaries"]:
            self.__delete_dependant_rows(keyword, tableName, keywordInfo["column_name"])

        # Delete keyword from reference table
        self.__delete_reference_row(keyword, keywordInfo["reference"])