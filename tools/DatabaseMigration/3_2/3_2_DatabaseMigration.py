# -*- coding: utf-8 -*-
#3_2_DatabaseMigration.py
#----------------------------------
# Created By: Anointiyae Beasley & Matthew Kastl
# Created Date:09/11/2025
# Version 1.0
#----------------------------------
"""This is a database migration script that will update to version
    3.2 of the database. The intended change is to make timeGenerated non-nullable, and delete null TimeGenerated rows in inputs
 """ 
#----------------------------------
# 
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from sqlalchemy import Engine, text


#Constants
CSV_FILE_PATHS = './tools/DatabaseMigration/3_2/init_data'


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 3.2 which update Source descriptions

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
           
            Updates DB:
            - Delete rows with null timeGenerated 
            - Make timeGenerated NOT NULL

        """

        delete_null_timeGenerated_stmt = text('DELETE FROM inputs  WHERE "generatedTime" IS NULL')
        timeGenerated_not_null_constraint = text('ALTER TABLE inputs ALTER COLUMN "generatedTime" SET NOT NULL')

        with databaseEngine.connect() as connection:
            # input table
            connection.execute(delete_null_timeGenerated_stmt)
            connection.execute(timeGenerated_not_null_constraint)

            connection.commit()
        return True


    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database back to version 3.1 which involves removing the changes 
           associated with version 3.2.

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """
        rollback_timeGenerated_not_null_constraint = text('ALTER TABLE inputs ALTER COLUMN "generatedTime" DROP NOT NULL')

        with databaseEngine.connect() as connection:
            # input table

            connection.execute(rollback_timeGenerated_not_null_constraint)

            connection.commit()
        return True
