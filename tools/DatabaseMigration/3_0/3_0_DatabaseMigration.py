# -*- coding: utf-8 -*-
#3_0_DatabaseMigration.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 3/20/2025
#----------------------------------
""" This migration adds a new column "ensembleMemberID" to the inputs and outputs tables.
    It also updates the existing AK constraints to include this new column.
    This is to support the new ensemble member functionality in the database to keep track
    of grouped ensemble member data.
 """ 
#----------------------------------
# 
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from sqlalchemy import Engine, text


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """ Preform this migration
           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """

        # For both input and output table
        # 1. Update table to add "ensembleMemberID" column (default value is null) of type integer
        # 2. Remove old AK, we aren't allowed to alter it
        # 3. Add new AK with the new column included
        inputs_add_emID_col_stmt = text('ALTER TABLE inputs ADD COLUMN "ensembleMemberID" integer')
        inputs_drop_AK00_stmt = text('ALTER TABLE inputs DROP  CONSTRAINT "inputs_AK00"')
        inputs_add_AK00_stmt = text("""
            ALTER TABLE inputs 
            ADD CONSTRAINT "inputs_AK00" 
            UNIQUE NULLS NOT DISTINCT (
                "isActual", 
                "generatedTime", 
                "verifiedTime", 
                "dataUnit", 
                "dataSource", 
                "dataLocation", 
                "dataSeries", 
                "dataDatum", 
                "latitude", 
                "longitude", 
                "ensembleMemberID" 
            )
        """)

        outputs_add_emID_col_stmt = text('ALTER TABLE outputs ADD COLUMN "ensembleMemberID" integer')
        outputs_drop_AK00_stmt = text('ALTER TABLE outputs DROP  CONSTRAINT "outputs_AK00"')
        outputs_add_AK00_stmt = text("""
            ALTER TABLE outputs
            ADD CONSTRAINT "outputs_AK00" 
            UNIQUE NULLS NOT DISTINCT (
                "timeGenerated", 
                "leadTime", 
                "modelName", 
                "modelVersion", 
                "dataLocation", 
                "dataSeries", 
                "dataDatum", 
                "ensembleMemberID"
            )
        """)        

        with databaseEngine.connect() as connection:
            # input table
            connection.execute(inputs_add_emID_col_stmt)
            connection.execute(inputs_drop_AK00_stmt)
            connection.execute(inputs_add_AK00_stmt)

            # input table
            connection.execute(outputs_add_emID_col_stmt)
            connection.execute(outputs_drop_AK00_stmt)
            connection.execute(outputs_add_AK00_stmt)

            connection.commit()
        return True
       
        

    def rollback(self, databaseEngine: Engine) -> bool:
        """ Rollback this migration.
           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """


        # For both input and output table
        # 1. Remove old AK, we aren't allowed to alter it. Ak references the column so
        #    we need to drop it first i think, it might do it automatically but I dont trust it >:|
        # 2. Add new AK with the new column removed
        # 3. Update table to drop "ensembleMemberID" 
        inputs_drop_AK00_stmt = text('ALTER TABLE inputs DROP  CONSTRAINT "inputs_AK00"')
        inputs_add_AK00_stmt = text("""
            ALTER TABLE inputs 
            ADD CONSTRAINT "inputs_AK00" 
            UNIQUE NULLS NOT DISTINCT (
                "isActual", 
                "generatedTime", 
                "verifiedTime", 
                "dataUnit", 
                "dataSource", 
                "dataLocation", 
                "dataSeries", 
                "dataDatum", 
                "latitude", 
                "longitude"
            )
        """)
        inputs_drop_emID_col_stmt = text('ALTER TABLE inputs DROP COLUMN "ensembleMemberID"')

        outputs_drop_AK00_stmt = text('ALTER TABLE outputs DROP  CONSTRAINT "outputs_AK00"')
        outputs_add_AK00_stmt = text("""
            ALTER TABLE outputs
            ADD CONSTRAINT "outputs_AK00" 
            UNIQUE NULLS NOT DISTINCT (
                "timeGenerated", 
                "leadTime", 
                "modelName", 
                "modelVersion", 
                "dataLocation", 
                "dataSeries", 
                "dataDatum"
            )
        """)  
        outputs_drop_emID_col_stmt = text('ALTER TABLE outputs DROP COLUMN "ensembleMemberID"')      

        with databaseEngine.connect() as connection:
            # input table
            connection.execute(inputs_drop_AK00_stmt)
            connection.execute(inputs_add_AK00_stmt)
            connection.execute(inputs_drop_emID_col_stmt)

            # input table
            connection.execute(outputs_drop_AK00_stmt)
            connection.execute(outputs_add_AK00_stmt)
            connection.execute(outputs_drop_emID_col_stmt)

            connection.commit()
        return True
        