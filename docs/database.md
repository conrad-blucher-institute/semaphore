
# Database

Semaphore currently uses PostgreSQL to store model outputs, metadata, and other information needed during execution. Database schema changes are managed through the migration system located in `DatabaseMigration/`.

## How Semaphore Connects to the Database

To connect, Semaphore uses the database credentials defined in your `.env` file.

Example:

- Host: `localhost`
- Port: `DB_HOST_PORT`
- Username: `POSTGRES_USER`
- Password: `POSTGRES_PASSWORD`
- Database: `POSTGRES_DB`

---

## Database Migration

`migrate_db.py` is used to initialize, upgrade, or roll back the Semaphore database. The tool checks `DatabaseMigration/target_version.json` to determine which database version should be installed and applies the necessary migrations.

### Creating a New Database Migration

1. Create a new version folder in `DatabaseMigration/` (for example, `3.7/`) and add the scripts needed to upgrade and roll back the database. Looking at previous migration folders is the easiest way to understand the structure.
2. Update `DatabaseMigration/target_version.json` with the new version number and update the comment to describe the changes.
3. Rebuild the Docker containers:

```bash
docker compose up -d --build
```

4. Run the migration tool:

```bash
docker exec semaphore-core python3 tools/migrate_db.py
```

5. Make sure the migration can also be rolled back. Change `target_version.json` back to the previous version and rerun the migration to verify everything works correctly.

### Notes

- Always test both upgrading and rolling back before merging a new migration.
- If you switch to a branch that uses an older database version, you may need to remove the Docker volumes before rebuilding. Otherwise, the existing database may not match the schema expected by that branch.

```bash
docker compose down --volumes
```

---


## Resetting the Database - LOCAL USE ONLY

If the database becomes corrupted or you switch to a branch that expects a different database version that doesn't exist, remove the existing Docker volumes and recreate the containers.

```bash
docker compose down --volumes
docker compose up -d --build
docker exec semaphore-core python3 tools/migrate_db.py
```

This creates a fresh database using the schema specified in `DatabaseMigration/target_version.json`.

---


## Viewing the Database Through pgAdmin

You can use pgAdmin to connect to the Semaphore PostgreSQL database and view or edit its contents.

### Registering a New Server

1. Open **pgAdmin**.
2. In the left sidebar, right-click **Servers** and select **Register → Server...**.
3. Under the **General** tab, give the connection a name (for example, `Semaphore Local`).

### Connection Settings

Switch to the **Connection** tab and fill in the following values using the credentials from your `.env` file:

| Field | Value |
|-------|-------|
| **Host name/address** | `localhost` |
| **Port** | `DB_HOST_PORT` |
| **Maintenance database** | `POSTGRES_DB` |
| **Username** | `POSTGRES_USER` |
| **Password** | `POSTGRES_PASSWORD` |

> Replace `DB_HOST_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, and `POSTGRES_PASSWORD` with the values from your `.env` file.

Click **Save** to register the server.

### Browsing the Database

After connecting, expand the following folders in the left sidebar:

```text
Servers
└── Semaphore Local
    └── Databases
        └── <POSTGRES_DB>
            └── Schemas
                └── public
                    └── Tables
```

From here you can:

- View table data by right-clicking a table and selecting **View/Edit Data → All Rows**.
- Inspect the table structure under **Columns**.
- Run custom SQL queries using **Tools → Query Tool**.

### Troubleshooting

If you cannot connect:

- Make sure the Docker containers are running:

```bash
docker compose ps
```

- Verify that the PostgreSQL container is healthy.
- Confirm that the values in pgAdmin match those in your `.env` file.
- Ensure port `5432` is not being used by another PostgreSQL instance.
- Ensure you are logged into Cisco if accessing sherlock-dev or sherlock-prod


---

## Database Monitoring
Semaphore uses PostgreSQL’s pg_stat_statements extension to track which SQL queries are being executed, how often they run, and how expensive they are. This is useful for identifying slow or frequently called queries that may need optimization.

- Replace POSTGRES_USER and POSTGRES_DB with real .env variables to access the db container: `docker compose exec db psql -U "POSTGRES_USER " -d "POSTGRES_DB "`
- Run command: `SHOW shared_preload_libraries;`
- The result should be "pg_stat_statements". If this is not listed, the database will not collect query statistics.
- This prints one row at a time in a readable format: \x on (To turn it off: \x off)
- Wait for traffic to run for a bit then query the statements to see the most expensive SQL queries:
SELECT
    query,
    calls,
    total_exec_time,
    mean_exec_time,
    max_exec_time
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 5;

- List query id's:
SELECT
  queryid,
  calls,
  round(total_exec_time::numeric, 2) AS total_ms,
  left(regexp_replace(query, '\s+', ' ', 'g'), 180) AS query_preview
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 10;



