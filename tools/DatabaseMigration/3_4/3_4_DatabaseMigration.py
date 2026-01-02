# -*- coding: utf-8 -*-
#3_4_DatabaseMigration.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 12/03/2025
#----------------------------------
"""Adding indexes to the inputs table.
 """ 
#----------------------------------
# 
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from DatabaseMigration.databaseMigrationUtility import KeywordType, DatabaseDeletionHelper
from sqlalchemy import Engine, text


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 3.4.

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """

        stmt_drop_idx_inputs_ordering = text('DROP INDEX IF EXISTS idx_inputs_ordering;')
        stmt_create_idx_inputs_ordering = text('CREATE INDEX idx_inputs_ordering ON inputs ("verifiedTime", "ensembleMemberID", "generatedTime" DESC);')
        with databaseEngine.connect() as connection:
            connection.execute(stmt_drop_idx_inputs_ordering)
            connection.execute(stmt_create_idx_inputs_ordering)
            connection.commit()
    
        return True


    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database back to version 3.3.

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful rollback
        """
        
        stmt_drop_idx_inputs_ordering = text('DROP INDEX IF EXISTS idx_inputs_ordering;')
        with databaseEngine.connect() as connection:
            connection.execute(stmt_drop_idx_inputs_ordering)
            connection.commit()
    
        return True