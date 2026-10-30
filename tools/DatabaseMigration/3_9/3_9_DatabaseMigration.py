"""
3.9 Database Migration

This migration drops the inputs_AK00 constraint from the inputs table,
then adds back the constraint with the lat/lon removed from the unique constraint

Created by: CJ Quintero
Created on: 10/30/2026
"""
from DatabaseMigration.IDatabaseMigration import IDatabaseMigration
from sqlalchemy import Engine, text

class Migrator(IDatabaseMigration):

    def update(self, engine: Engine) -> bool:
        """
        perform this migration

        Args:
            engine (Engine) - the semaphore database engine we are connecting to
        
        Returns:
            bool - indicating successful update
        """

        drop_stmt = text("""
            ALTER TABLE inputs
            DROP CONSTRAINT "inputs_AK00"
        """)

        add_stmt = text("""
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
                "ensembleMemberID"
            )
        """)

        with engine.connect() as conn:
            with conn.begin():
                conn.execute(drop_stmt)
                conn.execute(add_stmt)

        return True



    def rollback(self, engine: Engine) -> bool:
        """
        rollback this migration by dropping the new constraint
        and adding back the old one

        Args:
            engine (Engine) - the semaphore database engine we are connecting to
        
        Returns:
            bool - indicating successful rollback
        """

        drop_stmt = text("""
            ALTER TABLE inputs
            DROP CONSTRAINT "inputs_AK00"
        """)

        add_stmt = text("""
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

        with engine.connect() as conn:
            with conn.begin():
                conn.execute(drop_stmt)
                conn.execute(add_stmt)

        return True
