# -*- coding: utf-8 -*-
#4_0_DatabaseMigration.py
#----------------------------------
# Created By: Florence Tissot
# Created Date: 11/10/2026
#----------------------------------
"""Adds the inputs_archive table, which holds old inputs rows.

   The inputs table grows very fast (ensemble input vectors store one row per input vector member),
   which slows down every query that hits it. Old rows are moved out of inputs into inputs_archive by
   a separate script; this migration only creates the table.

   inputs_archive has the same columns as inputs, plus an archivedTime column that stored when a row 
   was inserted (presumably archived). The table also has deliberate differences from the inputs table:
     - id keeps the value the row had in inputs, so it has no sequence/default.
     - No primary key, no unique constraint, no foreign keys, and no indexes. No application code reads
       this table; it is only queried by hand. Indexes can be added later if those queries need them.
       Without a unique constraint, a row that was archived, then ingested again and archived again,
       will appear twice. This is to speed up any insert in this table.
 """
#----------------------------------
#
#
#Imports
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from sqlalchemy import Engine, text


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database to version 4.0 by creating the inputs_archive table.
           Column types match the inputs table as of version 3.8. archivedTime defaults to the start time
           of the transaction that archives the row, in UTC, so the archive script doesn't need to set it
           and every row moved in one batch gets the same value.

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update
        """

        stmt_create_inputs_archive = text('''
            CREATE TABLE inputs_archive (
                "id"               integer                     NOT NULL,
                "generatedTime"    timestamp without time zone NOT NULL,
                "acquiredTime"     timestamp without time zone NOT NULL,
                "verifiedTime"     timestamp without time zone NOT NULL,
                "dataValue"        character varying(25)       NOT NULL,
                "isActual"         boolean                     NOT NULL,
                "dataUnit"         character varying(10)       NOT NULL,
                "dataSource"       character varying(10)       NOT NULL,
                "dataLocation"     character varying(25)       NOT NULL,
                "dataSeries"       character varying(25)       NOT NULL,
                "dataDatum"        character varying(10),
                "latitude"         character varying(16),
                "longitude"        character varying(16),
                "ensembleMemberID" integer,
                "archivedTime"     timestamp without time zone NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
            );
        ''')
        with databaseEngine.connect() as connection:
            connection.execute(stmt_create_inputs_archive)
            connection.commit()

        return True


    def rollback(self, databaseEngine: Engine) -> bool:
        """This function rolls the database back to version 3.8 by dropping the inputs_archive table.

           The rollback refuses to run if inputs_archive contains any rows. This migration does not add
           rows, so it is not its job to decide what happens to rows that have been added. 

           The table is locked before the check so nothing can add rows between the check
           and the drop.

           :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful rollback
        """

        stmt_lock_inputs_archive = text('LOCK TABLE inputs_archive IN ACCESS EXCLUSIVE MODE;')
        stmt_archive_has_rows = text('SELECT EXISTS (SELECT 1 FROM inputs_archive);')
        stmt_drop_inputs_archive = text('DROP TABLE inputs_archive;')
        with databaseEngine.connect() as connection:
            connection.execute(stmt_lock_inputs_archive)
            if connection.execute(stmt_archive_has_rows).scalar():
                # No explicit rollback needed: nothing was modified, and leaving the with block closes the
                # connection, which ends the open transaction and releases the table lock.
                print('inputs_archive is not empty. Move or delete its rows before rolling back to 3.8.')
                return False
            connection.execute(stmt_drop_inputs_archive)
            connection.commit()

        return True
