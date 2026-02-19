# Semaphore

## Description

Semaphore is a student-built and maintained Python application that semi-automates the process of operationalizing AI models. Semaphore downloads data, runs models, post-process outputs, and saves outputs. Semaphore currently takes model data in the form of .H5 files and input specifications and a DSPEC JSON. Examples of both are included in this repository.

## Dependency
- Python version >= 3.10
- Some version of SQLite (deprecated) [SQLite Download Page](https://www.sqlite.org/download.html) OR
- PostGres 15.4 with SQLAlchemy 2.0.20  //you can use pgAdmin to access and view the PG database

## Deployment
- Requirements:
  - `Docker Desktop`
  - `docker-compose`
- Dependencies:
  - Python 3.10.5

0. You will need to create a `.env` file in the root directory of the project. This file will contain the environment variables for the docker container. You can copy the contents of `env.dist` and replace the values with your own.
1. Ensure that Docker Desktop is running on your machine
2. Run `docker compose build` and `docker compose up` (run `docker compose up -d` to run in the background)
3. Intitalize the database by running: `docker exec semaphore-core python3 tools/migrate_db.py`
4. Database can be accessed using pgAdmin or VSCode Postgresql Explorer plugin
 ![Register DB to pgAdmin](https://user-images.githubusercontent.com/7061990/268778360-2b92cdc0-19dd-48ae-853c-c52876f747d3.png)
 ![Enter your database credentials to register to pgAdmin](https://user-images.githubusercontent.com/7061990/268778380-36b907c7-da08-4ba8-a232-0c7646dfbb82.png)
5. To stop the containers: Press `ctrl+C` in the terminal or if running in background close in Docker Desktop (`docker compose down` also works)
6. If you are making changes to the code, be sure to `docker compose down` and `docker compose build` before `docker compose up`. If you are making changes to the database, be sure to delete the existing DB volume on Docker Desktop before building again

## Running Semaphore
Currently, you can run Semaphore through the make_and_save_prediction and make_prediction methods in the `src/semaphoreRunner.py` which contains documentation for the CLI. Keep in mind the DB will need to be loaded with the proper location mappings for your ingested data. Feel free to reach out to one of the authors for help getting Semaphore up and running.

## Running FastAPI Application
1. Make sure your environment is activated! `source ./venv/vsem/Scripts/activate`
2. `pip install -r requirements.txt` to install new dependencies (if you haven't already)
3. From the root directory, run `uvicorn src.API.apiDriver:app`
4. You can specify `--host` and `--port` if you so desire
5. uvicorn will give you a link to the index page of the API. Add `/docs` to the URL to access the docs page

## Semaphore DB Monitoring
Semaphore uses PostgreSQL’s pg_stat_statements extension to track which SQL queries are being executed, how often they run, and how expensive they are. This is useful for identifying slow or frequently called queries that may need optimization.

- Replace POSTGRES_USER and POSTGRES_DB with real .env variables to access the db container: `docker compose exec db psql -U "POSTGRES_USER " -d "POSTGRES_DB "`
- Run command: `SHOW shared_preload_libraries;`
- The result should be "pg_stat_statements". If this is not listed, the database will not collect query statistics.
- This prints one row at a time in a readable format: \x on (To turn it off: \x off
)
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

## Semaphore Tools
Semaphore has a tools folder with some useful tools for monitoring how the semaphore system is performing across dev and prod. 

### parse_semaphore_logs.sh
`parse_semaphore_logs.sh` is a bash utility that scans Semaphore model log files 
for a given date range and produces a CSV summarizing each model run — whether it 
succeeded, failed, and why.

#### Usage
```bash
./tools/parse_semaphore_logs.sh  
```

Dates must be in `MM/DD/YY` format. For example:
```bash
./tools/parse_semaphore_logs.sh 02/07/26 02/18/26
```

The script can be run from anywhere in the repo. It resolves all paths relative 
to its own location, so you don't need to `cd` anywhere first.

#### How It Works
1. **Selects log files by date range** — rather than scanning all logs, it builds 
   `YYYY_M_` filename patterns from your date range (e.g. `2026_2_*.log`) and only 
   opens files that could contain relevant entries. This keeps it fast even with 
   hundreds of model log directories.

2. **Parses each file line by line** — it looks for three kinds of log events:
   - Lines containing `completed successfully` → recorded as a `success`
   - Lines containing `DateRangeValidation:` → signals the start of a failure block, 
     and the script begins accumulating context lines
   - Lines containing `FAILED - Null result inserted` → closes the failure block and 
     writes a `failed` row, extracting the reason and details from the accumulated context

3. **Writes results to a timestamped CSV** saved in `parsed_semaphore_logs/` at the 
   repo root. The folder is created automatically if it doesn't exist.

4. **Prints a summary** to stdout when finished — total entries, success/failure 
   counts, success rate, top failure reasons, and which models failed most often.

#### Output
Results are saved to:
```
semaphore/parsed_semaphore_logs/semaphore_dev_stats_YYYYMMDD_HHMMSS.csv
```

The CSV has the following columns:

| Column | Description |
|---|---|
| `timestamp` | When the model run occurred (`MM/DD/YY HH:MM:SS`) |
| `model_name` | Name of the model that ran |
| `status` | `success` or `failed` |
| `failure_reason` | `missing_data`, `stale_data`, or `unknown` (empty on success) |
| `data_source` | The data source identified in the validation error, if present |
| `missing_data_details` | Structured detail string — may include `missing_count`, `times`, and/or `time_diff` fields separated by `\|` |
| `log_file` | The log filename the entry came from |
| `line_number` | Line number in that file where the event was detected |
`
To get the output CSV from the dev server to your home machine run the following commands: 
1. On sherlock-dev in the semaphore.svc `sudo cp /home/semaphore.svc/semaphore/tools/parsed_semaphore_logs/semaphore_dev_stats_[TIMESTAMP OF FILE].csv /home/[YOUR USERNAME].admin/`
2. On local machine `scp [YOUR USERNAME].admin@sherlock-dev:/home/[YOUR USERNAME].admin/semaphore_dev_stats_[TIMESTAMP OF FILE].csv [WHERE YOU WANT FILE TO GO] (EX: C:\Users\steph\Downloads\)`

To get the output CSV from the prod server to your home machine run the following commands: 
1. on sherlock-prod in the semaphore.svc `sudo cp /home/semaphore.svc/semaphore/tools/parsed_semaphore_logs/semaphore_dev_stats_[TIMESTAMP OF FILE].csv /home/[YOUR USERNAME]].admin/`
2. On local machine `scp [YOUR USERNAME].admin@sherlock-prod.tamucc.edu:/home/[YOUR USERNAME].admin/semaphore_dev_stats_[TIMESTAMP OF FILE].csv [WHERE YOU WANT FILE TO GO]`

## Authors
* [@Matthew Kastl](https://github.com/matdenkas) - mkastl@islander.tamucc.edu
* [@Beto Estrada](https://github.com/bestrada33) - bestrada4@islander.tamucc.edu
* [@Savannah Stephenson](https://github.com/lovelysandlonelys) - sstephenson2@islander.tamucc.edu
* [@Anointiyae Beasley](https://github.com/abeasley1722) - anointiyae.beasley@tamucc.edu
* [@Jeremiah Sosa](https://github.com/jeremiahsosa77) - jsosa14@islander.tamucc.edu
* [@Florence Tissot](https://github.com/ccftissot)

