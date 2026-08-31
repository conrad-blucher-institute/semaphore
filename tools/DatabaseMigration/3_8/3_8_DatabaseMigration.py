# -*- coding: utf-8 -*-
# 3_8_DatabaseMigration.py
# ----------------------------------
# Created By: CJ Quintero
# Created Date: 08/31/2026
# Version 1.0
# ----------------------------------
"""
This db migration script adds Seadrift, Aransas Wildlife Refuge, and ESB to the ref_dataLocation table
and adds Port Lavaca, Seadrift, and Aransas Wildlife Refuge to the dataLocation_dataSource_mapping table
for the operation of the ESB cold stunning model.
""" 
# ----------------------------------
# 
#
# Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from sqlalchemy import Engine
from sqlalchemy.sql import text
import csv

# Constants
MAPPING_CSV = './tools/DatabaseMigration/3_8/init_data/dataMapping.csv'
LOCATION_CSV = './tools/DatabaseMigration/3_8/init_data/dataLocation.csv'


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """
        This function updates the database to version 3.8 which adds rows to the
        dataLocation_dataSource_mapping table and the ref_dataLocation table.
        The rows added are read from the dataLocation.csv and dataMapping.csv files.

        :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)

        :return: bool indicating successful update

        NOTE: Both tables use strings as the data type for all values except for priorityOrder which
        is an integer.
        """
        # read the rows 
        location_rows, mapping_rows = self._read_rows()

        with databaseEngine.connect() as conn:
            # conn.begin() will commit a single transaction at the end of the with block
            with conn.begin():

                # insert rows into the location table
                for row in location_rows:
                    stmt = text("""
                    INSERT INTO "ref_dataLocation" (
                        "code",
                        "displayName",
                        "notes",
                        "latitude",
                        "longitude"
                    ) 
                    VALUES (
                        :code,
                        :displayName,
                        :notes,
                        :latitude,
                        :longitude
                    )
                    """)
                    bind_params = {
                        "code": row["code"],
                        "displayName": row["displayName"],
                        "notes": row["notes"],
                        "latitude": row["latitude"],
                        "longitude": row["longitude"]
                    }
                    stmt = stmt.bindparams(**bind_params)
                    conn.execute(stmt)

                # insert rows into the mapping table
                for row in mapping_rows:
                    stmt = text("""
                    INSERT INTO "dataLocation_dataSource_mapping" (
                        "dataLocationCode",
                        "dataSourceCode",
                        "dataSourceLocationCode",
                        "priorityOrder"
                    ) 
                    VALUES (
                        :dataLocationCode,
                        :dataSourceCode,
                        :dataSourceLocationCode,
                        :priorityOrder
                    )
                    """)
                    bind_params = {
                        "dataLocationCode": row["dataLocationCode"],
                        "dataSourceCode": row["dataSourceCode"],
                        "dataSourceLocationCode": row["dataSourceLocationCode"],
                        "priorityOrder": int(row["priorityOrder"])
                    }
                    stmt = stmt.bindparams(**bind_params)
                    conn.execute(stmt)
        
        return True
    
    def _read_rows(self) -> tuple[list[dict], list[dict]]:
        """
        This function reads rows from the dataLocation.csv and dataMapping.csv files and returns them
        as a list of dictionaries

        :return tuple[list[dict], list[dict]] - a tuple of 2 lists of dictionaries representing each row in the csv files.
            The first element is the list of dictionaries from the dataLocation csv file
            and the second element is the list of dictionaries from the dataMapping csv file.
        """

        # read rows that were added from the data location csv file
        with open(f'{LOCATION_CSV}', mode='r') as file:
            location_rows = [row for row in csv.DictReader(file)]

        # read rows that were added from the data mapping csv file
        with open(f'{MAPPING_CSV}', mode='r') as file:
            mapping_rows = [row for row in csv.DictReader(file)]

        return location_rows, mapping_rows


    def rollback(self, databaseEngine: Engine) -> bool:
        """
        This function rolls the database back to version 3.7 which involves removing the changes 
        associated with version 3.8. The added rows for the ref_dataLocation table and the
        dataLocation_dataSource_mapping table will be removed.

        :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)

        :return: bool indicating successful rollback
        """

        location_rows, mapping_rows = self._read_rows()

        with databaseEngine.connect() as conn:
            # conn.begin() will commit a single transaction at the end of the with block
            with conn.begin():

                # delete rows from the mapping table first
                for row in mapping_rows:
                    stmt = text("""
                        DELETE FROM "dataLocation_dataSource_mapping"
                        WHERE "dataLocationCode" = :code
                        AND "dataSourceCode" = :source
                    """)

                    bind_params = {
                        "code": row["dataLocationCode"],
                        "source": row["dataSourceCode"]
                    }

                    stmt = stmt.bindparams(**bind_params)
                    conn.execute(stmt)

                # then delete rows from the location table
                for row in location_rows:
                    stmt = text("""
                        DELETE FROM "ref_dataLocation"
                        WHERE "code" = :code
                    """)

                    bind_params = {
                        "code": row["code"]
                    }

                    stmt = stmt.bindparams(**bind_params)
                    conn.execute(stmt)

        return True