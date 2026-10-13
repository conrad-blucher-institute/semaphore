# Semaphore

## Description

Semaphore is a student-built and maintained Python application that semi-automates the process of operationalizing AI models. Semaphore downloads and postprocess' data, runs models, and saves the output to our database. Semaphore currently takes model data in the form of .H5, .keras file. Input specifications for each model are located in the DSPEC JSON. Examples are included in this repository.

## Dependency
- Python version >= 3.12
- Some version of SQLite (deprecated) [SQLite Download Page](https://www.sqlite.org/download.html) OR
- PostGres 15.4 with SQLAlchemy 2.0.20  //you can use pgAdmin to access and view the PG database

## Running Semaphore
- Requirements:
  - `Docker Desktop`
  - `docker-compose`
- Dependencies:
  - Python 3.12

0. You will need to create a `.env` file in the root directory of the project. This file will contain the environment variables for the docker container. You can copy the contents of `.env.dist` and replace the values with your own. Semaphore will not run successfully without the appropriate configuration. Feel free to reach out to one of the authors for help getting Semaphore up and running.

1. Ensure that Docker Desktop is running on your machine
2. Run `docker compose up --build` (run `docker compose up -d --build` to run in the background)
3. Intitalize the database by running: `docker exec semaphore-core python3 tools/migrate_db.py`
4. Database can be accessed using pgAdmin or VSCode Postgresql Explorer plugin
 ![Register DB to pgAdmin](https://user-images.githubusercontent.com/7061990/268778360-2b92cdc0-19dd-48ae-853c-c52876f747d3.png)
 ![Enter your database credentials to register to pgAdmin](https://user-images.githubusercontent.com/7061990/268778380-36b907c7-da08-4ba8-a232-0c7646dfbb82.png)
5. To stop the containers: Press `ctrl+C` in the terminal or if running in background close in Docker Desktop (`docker compose down` also works)
6. If you are making changes to the code, be sure to repeat step 2. If you are making changes to the database, be sure to delete the existing DB volume using `docker compose down --volumes`(Only use this command LOCALLY) on Docker Desktop before repeating step 2.
7. Run Semaphore with the following command: `docker exec semaphore-core python3 -m semaphore.semaphoreRunner -d data/dspec/ColdStunning/Bird-Island_Water-Temperature_12hr.json`.
Each DSPEC corresponds to a specific model. To run a model in Semaphore, provide the DSPEC associated with that model. DSPEC paths are relative to the root of the repository, which is `/app` inside the containers.

Find common commands used in Semaphore here: https://github.com/conrad-blucher-institute/semaphore/wiki/Common-Commands

## Running FastAPI Application
1. Make sure your environment is activated! `source ./venv/vsem/Scripts/activate`
2. `pip install -e .` to install the `semaphore` package and its dependencies (if you haven't already)
3. From the root directory, run `uvicorn apiDriver:app --app-dir api`
4. You can specify `--host` and `--port` if you so desire
5. uvicorn will give you a link to the index page of the API. Add `/docs` to the URL to access the docs page

## Running Semaphore Locally (outside Docker)
Useful for debugging. Semaphore runs from your virtual environment and uses the database container.

One-time setup, from the root directory:
1. `pip install -e '.[test]'` installs the `semaphore` package in editable mode (code changes are picked up without reinstalling) plus the test dependencies.
2. On Linux, add `host.docker.internal` to your hosts file: `echo '127.0.0.1 host.docker.internal' | sudo tee -a /etc/hosts`. `.env` points the database at `host.docker.internal`, which otherwise only resolves inside the containers.

Each time, from the root directory:
1. Start the database container: `docker compose up -d db`
2. Run Semaphore, e.g. with `-t` so the prediction isn't saved:
   `python -m semaphore.semaphoreRunner -d data/dspec/ColdStunning/Bird-Island_Water-Temperature_12hr.json -t`

   Logs are written under `./data/logs/`. Only failures are logged unless you add `-v`.

Run from the root directory: the folder settings in `.env` (`MODEL_FOLDER_PATH`, `LOG_BASE_PATH`) are relative paths, so they resolve against the folder you run from. That is the root directory on your machine and `/app` in the containers, so the same values work in both. If your `.env` was copied from an older `.env.dist` and still has `/app/...` values, update them to match `.env.dist`.

## Running the Unit Tests
The unit tests run with pytest. `pyproject.toml` points pytest at `tests/UnitTests`, so no path is needed.

- In the core container: `docker exec semaphore-core python3 -m pytest`
- Locally, after the one-time setup in [Running Semaphore Locally](#running-semaphore-locally-outside-docker) and with the database container running: `python -m pytest`
- Add `--run-slow` to also run the tests marked as slow, or `--only-slow` to run only those. They run real models, using the same relative folder settings as Semaphore.
- Run pytest from the root directory. Some tests find their files with paths relative to it.
- `tests/IntegrationTests` is not run by default; run a file there by naming it, e.g. `python -m pytest tests/IntegrationTests/test_API.py`.
- To run all tests, specify a path when running pytest, e.g., `python3 -m pytest tests/`.  Again, make sure to run pytest from the root folder (semaphore locally, or root in the container)


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
To get the output CSV from the dev server to your home machine please see the Private Wiki: https://github.com/conrad-blucher-institute/semaphore-private-wiki/wiki/Semaphore-Log-Parsing-Download-Instructions

## Authors
* [@Matthew Kastl](https://github.com/matdenkas) - mkastl@islander.tamucc.edu
* [@Beto Estrada](https://github.com/bestrada33) - bestrada4@islander.tamucc.edu
* [@Savannah Stephenson](https://github.com/lovelysandlonelys) - sstephenson2@islander.tamucc.edu
* [@Anointiyae Beasley](https://github.com/abeasley1722) - anointiyae.beasley@tamucc.edu
* [@Christian Quintero](https://github.com/CJQuintero61) - 
* [@Jeremiah Sosa](https://github.com/jeremiahsosa77) - jsosa14@islander.tamucc.edu
* [@Florence Tissot](https://github.com/ccftissot)

