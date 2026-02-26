# -*- coding: utf-8 -*-
#3_6_DatabaseMigration.py
#----------------------------------
# Created By: Anointiyae Beasley
# Created Date: 02/25/2026
# Version 1.0
#----------------------------------
"""This is a database migration script that will initialize version
    3.6 of the database (without using the ORM). It will rename the existing outputs table and create the new outputs table.
    In the new table ensemble member id will be removed and dataValue will use BYTEA values.
"""
#----------------------------------

from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from sqlalchemy import Engine, text


class Migrator(IDatabaseMigration):

    def update(self, databaseEngine: Engine) -> bool:
        """This function updates the database by retiring the current outputs table and replacing it with a new one that does not include ensemble member id and uses BYTEA for dataValue.
            We also retire the model_runs table so that, during rollback, we restore the original model_runs and outputs pair together.
            Otherwise, model_runs might contain outputID values that do not exist in the restored outputs table, leading to foreign key errors.
          
            :param databaseEngine: Engine - the engine of the database we are connecting to (semaphore)
           :return: bool indicating successful update

           Updates DB:
            - Rename output table to retired_output
                 - Rename all constraints
                 - Rename id sequence 
            - Rename model_runs to retired_model_runs
                - Rename all constraints
                - Rename id sequence
            - Create the new outputs table
                - Removes enemble member id
                - dataValue uses BYTEA
                - Add AK00 constraints 
            - Create the new model_run table

            Rollback:
            - Drop new outputs table and its constraints, sequence, and FK
            - Restore original outputs table
                - Rename retired constraints back to original
            - Restore original model runs
                - Rename retired constraints back to original
        """
        with databaseEngine.connect() as connection:

            # Rename outputs -> retired_outputs
            connection.execute(text('ALTER TABLE public."outputs" RENAME TO "retired_outputs";'))

            # Rename constraints on retired_outputs 
            connection.execute(text('ALTER TABLE public."retired_outputs" RENAME CONSTRAINT "outputs_pkey" TO "retired_outputs_pkey";'))
            connection.execute(text('ALTER TABLE public."retired_outputs" RENAME CONSTRAINT "outputs_AK00" TO "retired_outputs_AK00";'))
            connection.execute(text('ALTER TABLE public."retired_outputs" RENAME CONSTRAINT "outputs_dataDatum_fkey" TO "retired_outputs_dataDatum_fkey";'))
            connection.execute(text('ALTER TABLE public."retired_outputs" RENAME CONSTRAINT "outputs_dataLocation_fkey" TO "retired_outputs_dataLocation_fkey";'))
            connection.execute(text('ALTER TABLE public."retired_outputs" RENAME CONSTRAINT "outputs_dataSeries_fkey" TO "retired_outputs_dataSeries_fkey";'))
            connection.execute(text('ALTER TABLE public."retired_outputs" RENAME CONSTRAINT "outputs_dataUnit_fkey" TO "retired_outputs_dataUnit_fkey";'))

            # Rename outputs_id_seq -> retired_outputs_id_seq and reattach
            connection.execute(text("""
            DO $$
            BEGIN
                IF to_regclass('public.outputs_id_seq') IS NOT NULL AND to_regclass('public.retired_outputs_id_seq') IS NULL THEN
                    EXECUTE 'ALTER SEQUENCE public.outputs_id_seq RENAME TO retired_outputs_id_seq';
                END IF;

                IF to_regclass('public.retired_outputs_id_seq') IS NOT NULL THEN
                    EXECUTE 'ALTER TABLE public."retired_outputs" ALTER COLUMN "id" SET DEFAULT nextval(''public.retired_outputs_id_seq'')';
                    EXECUTE 'ALTER SEQUENCE public.retired_outputs_id_seq OWNED BY public."retired_outputs"."id"';
                END IF;
            END $$;
            """))

            # Rename model_runs to retired_model_runs.
            
            connection.execute(text("""
                ALTER TABLE public."model_runs"
                RENAME TO "retired_model_runs";
            """))
            # Retire model runs constraints
            connection.execute(text('ALTER TABLE public."retired_model_runs" RENAME CONSTRAINT "model_runs_pkey" TO "retired_model_runs_pkey";'))
            connection.execute(text('ALTER TABLE public."retired_model_runs" RENAME CONSTRAINT "model_runs_AK00" TO "retired_model_runs_AK00";'))
            connection.execute(text('ALTER TABLE public."retired_model_runs" RENAME CONSTRAINT "model_runs_outputID_fkey" TO "retired_model_runs_outputID_fkey";'))

            #Retire model runs sequence
            connection.execute(text("""
                DO $$
                BEGIN
                    IF to_regclass('public.model_runs_id_seq') IS NOT NULL
                    AND to_regclass('public.retired_model_runs_id_seq') IS NULL THEN

                        EXECUTE 'ALTER SEQUENCE public.model_runs_id_seq RENAME TO retired_model_runs_id_seq';

                        EXECUTE 'ALTER TABLE public."retired_model_runs"
                                ALTER COLUMN "id"
                                SET DEFAULT nextval(''public.retired_model_runs_id_seq'')';

                        EXECUTE 'ALTER SEQUENCE public.retired_model_runs_id_seq
                                OWNED BY public."retired_model_runs"."id"';
                    END IF;
                END $$;
             """))

            # Create NEW outputs table (no ensembleMemberId, dataValue BYTEA)
            connection.execute(text("""
                CREATE TABLE public."outputs" (
                    "id" INTEGER GENERATED BY DEFAULT AS IDENTITY,
                    "timeGenerated" TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    "leadTime" INTERVAL NOT NULL,
                    "modelName" VARCHAR(50) NOT NULL,
                    "modelVersion" VARCHAR(10) NOT NULL,
                    "dataValue" BYTEA NOT NULL,
                    "dataUnit" VARCHAR(10) NOT NULL,
                    "dataLocation" VARCHAR(25) NOT NULL,
                    "dataSeries" VARCHAR(25) NOT NULL,
                    "dataDatum" VARCHAR(10),

                    CONSTRAINT "outputs_pkey" PRIMARY KEY ("id"),

                    CONSTRAINT "outputs_AK00"
                        UNIQUE NULLS NOT DISTINCT (
                            "timeGenerated", "leadTime", "modelName", "modelVersion",
                            "dataLocation", "dataSeries", "dataDatum"
                        ),

                    CONSTRAINT "outputs_dataUnit_fkey"
                        FOREIGN KEY ("dataUnit") REFERENCES public."ref_dataUnit" ("code"),

                    CONSTRAINT "outputs_dataLocation_fkey"
                        FOREIGN KEY ("dataLocation") REFERENCES public."ref_dataLocation" ("code"),

                    CONSTRAINT "outputs_dataSeries_fkey"
                        FOREIGN KEY ("dataSeries") REFERENCES public."ref_dataSeries" ("code"),

                    CONSTRAINT "outputs_dataDatum_fkey"
                        FOREIGN KEY ("dataDatum") REFERENCES public."ref_dataDatum" ("code")
                );
        """))
            
            # Create new model runs table
            connection.execute(text("""
                CREATE TABLE public."model_runs" (
                    "id" SERIAL NOT NULL,
                    "outputID" INTEGER NOT NULL,
                    "executionTime" TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    "returnCode" INTEGER NOT NULL,

                    CONSTRAINT "model_runs_pkey" PRIMARY KEY ("id"),

                    CONSTRAINT "model_runs_AK00"
                        UNIQUE ("outputID"),

                    CONSTRAINT "model_runs_outputID_fkey"
                        FOREIGN KEY ("outputID")
                        REFERENCES public."outputs" ("id")
                );
            """))
           
            connection.commit()
            return True

    def rollback(self, databaseEngine: Engine) -> bool:
        """ Rollback behavior:
            - Drop NEW model_runs
            - Drop NEW outputs
            - Restore original outputs table and constraints
            - Restore original model runs and constraints
        """
        with databaseEngine.connect() as connection:

            # Drop the NEW model runs and its constraints and sequence
            connection.execute(text('DROP TABLE IF EXISTS public."model_runs" CASCADE;'))

            # Drop the NEW outputs table and it's constraints and sequence.
            connection.execute(text('DROP TABLE IF EXISTS public."outputs" CASCADE;'))

            # Restore retired_outputs -> outputs
            connection.execute(text('ALTER TABLE public."retired_outputs" RENAME TO "outputs";'))

            # Restore constraint names
            connection.execute(text('ALTER TABLE public."outputs" RENAME CONSTRAINT "retired_outputs_pkey" TO "outputs_pkey";'))
            connection.execute(text('ALTER TABLE public."outputs" RENAME CONSTRAINT "retired_outputs_AK00" TO "outputs_AK00";'))
            connection.execute(text('ALTER TABLE public."outputs" RENAME CONSTRAINT "retired_outputs_dataDatum_fkey" TO "outputs_dataDatum_fkey";'))
            connection.execute(text('ALTER TABLE public."outputs" RENAME CONSTRAINT "retired_outputs_dataLocation_fkey" TO "outputs_dataLocation_fkey";'))
            connection.execute(text('ALTER TABLE public."outputs" RENAME CONSTRAINT "retired_outputs_dataSeries_fkey" TO "outputs_dataSeries_fkey";'))
            connection.execute(text('ALTER TABLE public."outputs" RENAME CONSTRAINT "retired_outputs_dataUnit_fkey" TO "outputs_dataUnit_fkey";'))

            # Restore sequence name outputs_id_seq and reattach 
            connection.execute(text("""
            DO $$
            BEGIN
                IF to_regclass('public.retired_outputs_id_seq') IS NOT NULL AND to_regclass('public.outputs_id_seq') IS NULL THEN
                    EXECUTE 'ALTER SEQUENCE public.retired_outputs_id_seq RENAME TO outputs_id_seq';
                END IF;

                IF to_regclass('public.outputs_id_seq') IS NOT NULL THEN
                    EXECUTE 'ALTER TABLE public."outputs" ALTER COLUMN "id" SET DEFAULT nextval(''public.outputs_id_seq'')';
                    EXECUTE 'ALTER SEQUENCE public.outputs_id_seq OWNED BY public."outputs"."id"';
                END IF;
            END $$;
            """))

            # Restore original model runs
            connection.execute(text('ALTER TABLE public."retired_model_runs" RENAME TO "model_runs";'))

            # Restore original model_runs constraint names
            connection.execute(text("""
            DO $$
            DECLARE
                r RECORD;
                new_name TEXT;
            BEGIN
                -- Rename any constraints that start with 'retired_' back to original (strip prefix)
                FOR r IN
                    SELECT conname
                    FROM pg_constraint
                    WHERE conrelid = 'public."model_runs"'::regclass
                    AND conname LIKE 'retired\\_%' ESCAPE '\\'
                LOOP
                    new_name := substring(r.conname from 9); -- remove 'retired_' (8 chars)
                    EXECUTE format('ALTER TABLE public."model_runs" RENAME CONSTRAINT %I TO %I;', r.conname, new_name);
                END LOOP;
            END $$;
            """))

            # Restore original model_runs sequence name 
            connection.execute(text("""
            DO $$
            BEGIN
                IF to_regclass('public.retired_model_runs_id_seq') IS NOT NULL
                AND to_regclass('public.model_runs_id_seq') IS NULL THEN
                    EXECUTE 'ALTER SEQUENCE public.retired_model_runs_id_seq RENAME TO model_runs_id_seq';
                END IF;

                IF to_regclass('public.model_runs_id_seq') IS NOT NULL THEN
                    EXECUTE 'ALTER TABLE public."model_runs" ALTER COLUMN "id" SET DEFAULT nextval(''public.model_runs_id_seq'')';
                    EXECUTE 'ALTER SEQUENCE public.model_runs_id_seq OWNED BY public."model_runs"."id"';
                END IF;
            END $$;
            """))

            connection.commit()
            return True