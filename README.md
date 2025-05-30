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

## Authors
* [@Matthew Kastl](https://github.com/matdenkas) - mkastl@islander.tamucc.edu
* [@Beto Estrada](https://github.com/bestrada33) - bestrada4@islander.tamucc.edu
* [@Savannah Stephenson](https://github.com/lovelysandlonelys) - sstephenson2@islander.tamucc.edu
* [@Anointiyae Beasley](https://github.com/abeasley1722) - anointiyae.beasley@tamucc.edu
* [@Jeremiah Sosa](https://github.com/jeremiahsosa77) - jsosa14@islander.tamucc.edu
* [@Florence Tissot](https://github.com/ccftissot)

