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

NOTE: The ref_dataLocation table depends on the mapping table, inputs table, and the outputs table.
The rows in the location csv and mapping csv are not the same, so the rollback
will first delete rows from the inputs and outputs table based on the dataLocation csv,
then delete the rows from the mapping table based on the dataMapping csv, then finally
delete the rows from the ref_dataLocation table based on the dataLocation csv.
""" 
# ----------------------------------
# 
#
# Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from DatabaseMigration.databaseMigrationUtility import KeywordType, DatabaseDeletionHelper
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

                # this creates many VALUES clauses to insert all rows in a single statement such as:
                # (:code0, :displayName0, :notes0, :latitude0, :longitude0),
                # (:code1, :displayName1, :notes1, :latitude1, :longitude1), ...
                values_clause = ", ".join(
                    f"(:code{i}, :displayName{i}, :notes{i}, :latitude{i}, :longitude{i})"
                    for i in range(len(location_rows))
                )

                stmt = text(f"""
                INSERT INTO "ref_dataLocation" (
                    "code",
                    "displayName",
                    "notes",
                    "latitude",
                    "longitude"
                )
                VALUES {values_clause}
                """)

                bind_params = {}
                for i, row in enumerate(location_rows):
                    bind_params[f"code{i}"] = row["code"]
                    bind_params[f"displayName{i}"] = row["displayName"]
                    bind_params[f"notes{i}"] = row["notes"]
                    bind_params[f"latitude{i}"] = row["latitude"]
                    bind_params[f"longitude{i}"] = row["longitude"]

                # perform a single insert statement for all rows
                stmt = stmt.bindparams(**bind_params)
                conn.execute(stmt)

                # dynamically create the values clause for the mapping table
                values_clause = ", ".join(
                    f"(:dataLocationCode{i}, :dataSourceCode{i}, :dataSourceLocationCode{i}, :priorityOrder{i})"
                    for i in range(len(mapping_rows))
                )

                stmt = text(f"""
                INSERT INTO "dataLocation_dataSource_mapping" (
                    "dataLocationCode",
                    "dataSourceCode",
                    "dataSourceLocationCode",
                    "priorityOrder"
                )
                VALUES {values_clause}
                """)

                bind_params = {}
                for i, row in enumerate(mapping_rows):
                    bind_params[f"dataLocationCode{i}"] = row["dataLocationCode"]
                    bind_params[f"dataSourceCode{i}"] = row["dataSourceCode"]
                    bind_params[f"dataSourceLocationCode{i}"] = row["dataSourceLocationCode"]
                    bind_params[f"priorityOrder{i}"] = int(row["priorityOrder"])

                # perform a single insert statement for all rows
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
        associated with version 3.8. The rollback will delete rows from the inputs, outputs, mapping,
        and location tables based on the dataLocation.csv file.

        :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)

        :return: bool indicating successful update

        NOTE: In the future we should update our helper functions to perform a single
        commit for all deletions instead of committing after each deletion. It is
        NOT RECOMMENDED to continue using deep_delete_keyword from the helper
        until it can be refactored.
        """
        # Note that we only explicitly include the locations csv here
        # because a part of the deep_delete protocol of the data locations
        # table is to delete the location from the mapping table. 
        fileNames = ['dataLocation.csv']
        fileTypes = [KeywordType.DATA_LOCATION]

        location_rows, _ = self._read_rows()

        # Using the utility helper class to delete any data dependent on the rows added in the 3.7 Migration
        helper = DatabaseDeletionHelper(databaseEngine)

        for file, type in zip(fileNames, fileTypes):
            for rowDict in location_rows:
                helper.deep_delete_keyword(rowDict["code"], type)

        # since Port Lavaca and Port O Connor were added to the mapping table
        # but not the location table, we need to delete them individually from the mapping table
        # NOTE: This doesn't remove entries in the inputs table that use these mappings
        helper.delete_mapping_row('PortLavaca', 'NOAATANDC')
        helper.delete_mapping_row('PortOConnor', 'NOAATANDC')

        return True