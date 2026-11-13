#!/usr/bin/env python
#archive_old_inputs.py
#-------------------------------
# Created By : Florence Tissot
# Created Date: 11/10/2026
# version 1.0
#-------------------------------
""" Moves rows from the inputs table to the inputs_archive table (added in DB migration 4.0).

    Allows caller to specify a custom cutoff date/time with --before but is not required.  If not
    provided, defaults to one month before the current UTC time. If provided, must be earlier than 
    the default cutoff.

    Each run is a single statement: the rows are deleted from inputs and inserted into inputs_archive
    in the same transaction, so a row is never in both tables and nothing is lost if the transaction
    is rolled back.

    This script does not reclaim disk space. Postgres reuses the space freed by the deletes for new
    inserts, so the weekly run keeps the table from growing on disk. Shrinking the files after the
    initial backlog is a separate, one-time step that must be handled separately.

    Defaults running as a dry run to ensure explicit confirmation before making changes to the database.
    Dry run means that the script performs all operations, report the row count and duration, but 
    rolls back the transaction instead of committing it. Use the --commit flag to force 
    the script to commit the changes.

    Run from the repo root:
        python tools/archive_old_inputs.py --commit
        python tools/archive_old_inputs.py --before 2025-11-01 --commit
        python tools/archive_old_inputs.py --before 2025-11-01  # measures only, changes nothing
 """
#-------------------------------
#
#
import argparse
import os
from datetime import datetime
from time import perf_counter

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Columns copied from inputs to inputs_archive. archivedTime is not listed: it defaults to the
# transaction's start time (UTC), so every row moved in one run gets the same archivedTime.
INPUTS_COLUMNS = '''
    "id", "generatedTime", "acquiredTime", "verifiedTime", "dataValue", "isActual",
    "dataUnit", "dataSource", "dataLocation", "dataSeries", "dataDatum",
    "latitude", "longitude", "ensembleMemberID"
'''

# inputs stores naive UTC timestamps, so the cutoff is computed the same way
STMT_DEFAULT_CUTOFF = text("SELECT (now() AT TIME ZONE 'UTC') - interval '1 month'")

STMT_MOVE_ROWS = text(f'''
    WITH moved AS (
        DELETE FROM inputs
        WHERE "generatedTime" < :cutoff
        RETURNING {INPUTS_COLUMNS}
    )
    INSERT INTO inputs_archive ({INPUTS_COLUMNS})
    SELECT {INPUTS_COLUMNS} FROM moved
''')


def parse_arguments():
    parser = argparse.ArgumentParser(description='Move inputs rows older than one month (by generatedTime) to inputs_archive.')
    parser.add_argument('-b', '--before', type=datetime.fromisoformat, required=False,
                        help='Archive rows generated before this UTC date/time (e.g. 2025-11-01). '
                             'Must not be later than the default cutoff of now minus one month.')
    parser.add_argument('--commit', action='store_true',
                        help='Do the move, report the row count and duration, then commit instead of rolling back. '
                             'Use it to actually apply the changes to the database.')
    return parser.parse_args()


def main():
    args = parse_arguments()

    load_dotenv()
    engine = create_engine(os.getenv('DB_LOCATION_STRING'))

    with engine.connect() as connection:
        default_cutoff = connection.execute(STMT_DEFAULT_CUTOFF).scalar()

        cutoff = default_cutoff
        if args.before is not None:
            if args.before.tzinfo is not None:
                raise ValueError('--before must be a UTC date/time without a time zone, e.g. 2025-11-01.')
            if args.before > default_cutoff:
                raise ValueError(f'--before {args.before} is later than the default cutoff {default_cutoff}; '
                                 'rows younger than one month should never be archived.')
            cutoff = args.before

        print(f'Archiving inputs rows with generatedTime before {cutoff} (UTC)')

        start = perf_counter()
        moved_count = connection.execute(STMT_MOVE_ROWS, {'cutoff': cutoff}).rowcount
        moved_elapsed = perf_counter() - start

        if not args.commit:
            connection.rollback()
        else:
            connection.commit()
        total_elapsed = perf_counter() - start

    if not args.commit:
        print(f'DRY RUN: moved {moved_count} rows to inputs_archive in {moved_elapsed:.1f} seconds, '
              f'then rolled back (nothing changed; {total_elapsed:.1f} seconds including the rollback)')
    else:
        print(f'Moved {moved_count} rows to inputs_archive in {total_elapsed:.1f} seconds')


if __name__ == '__main__':
    main()
