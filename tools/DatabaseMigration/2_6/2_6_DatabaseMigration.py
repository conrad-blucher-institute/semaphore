# -*- coding: utf-8 -*-
#2_6_DatabaseMigration.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date:10/07/2024
# Version 1.0
#----------------------------------
"""This is a database migration script that will update to version
    2.6 of the database. The intended change is to add a meta table
    to log info about predictions as well as making outputs nullable
 """ 
#----------------------------------
# 
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, UniqueConstraint, Engine, ForeignKey, Boolean, Interval, text
from sqlalchemy.dialects.postgresql import insert


#Implementing methods of interface
class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 2.6 which adds a model runs table and allows for nulled dataValues in the outputs table.
            :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
            :return: bool indicating successful update
        """

        self._metadata = MetaData()
        self._metadata.reflect(bind=databaseEngine)

        # This holds more information about an output
        self.model_runs = Table(
            "model_runs",
            self._metadata,

            Column("id", Integer, autoincrement=True, primary_key=True),
            Column("outputID", Integer, ForeignKey("outputs.id"),nullable=False),  
            Column("executionTime", DateTime, nullable=False), 
            Column("returnCode", Integer, nullable=False),                                                               

            UniqueConstraint("outputID", name='model_runs_AK00'),
        )

        self._metadata.create_all(databaseEngine) # Alter the scheme with the new table

        with databaseEngine.connect() as connection:
            # Make the dataValue column in outputs nullable
            connection.execute(text('ALTER TABLE %s ALTER COLUMN "%s" DROP NOT NULL' % ('outputs', 'dataValue')))
            connection.commit()

        return True


    def rollback(self, databaseEngine: Engine) -> bool:
        
        with databaseEngine.connect() as connection:
            # Make the dataValue column in prevent nulls
            connection.execute(text('ALTER TABLE %s ALTER COLUMN "%s" SET NOT NULL' % ('outputs', 'dataValue')))
            connection.commit()

        # Drop the model_runs table. No other table at this point has it as a fk
        self._metadata = MetaData()
        self._metadata.reflect(bind=databaseEngine)
        self._metadata.tables['model_runs'].drop(databaseEngine)

        return True
    

     