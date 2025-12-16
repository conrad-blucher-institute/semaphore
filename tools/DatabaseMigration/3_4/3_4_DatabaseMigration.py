# -*- coding: utf-8 -*-
#3_2_DatabaseMigration.py
#----------------------------------
# Created By: Anointiyae Beasley & Matthew Kastl
# Created Date:09/11/2025
# Version 1.0
#----------------------------------
"""This is a database migration script that will update to version 3.4 of the database. This migration enables pg_stat_statements at the database level by creating the extension.
Rollback drops the extension, removing the SQL interface. Postgres may still preload the module at startup, but no stats are accessible from the DB after rollback.
 """ 
#----------------------------------
# 
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from sqlalchemy import Engine, text



class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database by enabling pg_stat_statements.

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update

           Updates DB:
           - Creates pg_stat_statements extension if it does not exist
        """

        create_extension_stmt = text(
            'CREATE EXTENSION IF NOT EXISTS pg_stat_statements'
        )

        with databaseEngine.connect() as connection:
            connection.execute(create_extension_stmt)
            connection.commit()

        return True


    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database back by removing pg_stat_statements.

           NOTE:
           - This removes the SQL interface (view/functions) from the database
           - If pg_stat_statements is still preloaded in Postgres, stats may still
             be collected internally but will no longer be queryable.

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful rollback
        """

        drop_extension_stmt = text(
            'DROP EXTENSION IF EXISTS pg_stat_statements'
        )

        with databaseEngine.connect() as connection:
            connection.execute(drop_extension_stmt)
            connection.commit()

        return True
