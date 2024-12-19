# -*- coding: utf-8 -*-
#2_7_DatabaseMigration.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date:12/19/2024
# Version 1.0
#----------------------------------
"""This is a database migration script that will update to version
    2.7 of the database. The indended change is to raise the allowed
    length of the model name to 50 characters
 """ 
#----------------------------------
# 
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from sqlalchemy import Engine, text
from sqlalchemy.dialects.postgresql import insert


#Implementing methods of interface
class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """ Update to new version of the database
            :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
            :return: bool indicating successful update
        """

        with databaseEngine.connect() as connection:
            # Update modelName to allow for 50 characters
            connection.execute(text('ALTER TABLE %s ALTER COLUMN "%s" TYPE VARCHAR(50)' % ('outputs', 'modelName')))
            connection.commit()

        return True


    def rollback(self, databaseEngine: Engine) -> bool:

        with databaseEngine.connect() as connection:
            # Remove any model runes which are joined on the id row of model
            connection.execute(text('DELETE FROM %s WHERE model_runs."outputID" IN (SELECT id FROM %s WHERE LENGTH("%s") > 25)' % ('model_runs', 'outputs', 'modelName')))
            connection.commit()
            # Remove any output rows with a model name over 25 characters
            connection.execute(text('DELETE FROM %s WHERE LENGTH("%s") > 25' % ('outputs', 'modelName')))
            connection.commit()
            # Undo the change to modelName returning to a length of 25
            connection.execute(text('ALTER TABLE %s ALTER COLUMN "%s" TYPE VARCHAR(25)' % ('outputs', 'modelName')))
            connection.commit()

        return True
    

     