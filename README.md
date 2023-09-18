# Semaphore

## Description

Semaphore is a student-built and maintained Python application that semi-automates the process of operationalizing AI models. Semaphore downloads data, runs models, post-process outputs, and saves outputs. Semaphore currently takes model data in the form of .H5 files and input specifications and a DSPEC JSON. Examples of both are included in this repository.

## Dependency
Right now the only external dependency that is not included in the requirements file, is some version of SQLite as this is the only Database system currently supported.
[SQLite Download Page](https://www.sqlite.org/download.html)
    
## Deployment

1. Construct `env.dist`    
    - Create the `.env` file at the root directory of Semaphore, and copy the text from env.dict into it.
    - Assign DB_LOCATION_STRING. It maps your database file locations. It Should look like this `sqlite:///{YOUR FILE PATH}{your db file name}``. You do not already need a DB file in place, it will create it at the location when run for the first time.
    - (Optional) DSPEC_FOLDER_PATH & MODEL_FOLDER_PATH can either be relative paths from the working directory(default) or you can override with absolute paths.
2. Create virtual environment & install required Python dependencies
```python
python3 -m venv ./venv/vsem
source ./venv/vsem/Scripts/activate
pip install -r requirements.txt
```
(*NOTE:: depends on the python version, you may want to use `source ./venv/vsem/bin/activate)` instead)

(**NOTE:: You might want to use git bash if your terminal isn't recognizing `source`)

(***NOTE:: TensorFlow currently CPU build only.)

## Docker Container Development Setup
- Requirements:
  - `Docker Desktop`
  - `docker-compose`
- Dependencies:
  - Python 3.10.5

0. You will need to create a `.env` file in the root directory of the project. This file will contain the environment variables for the docker container. You can copy the contents of `env.dist` and replace the values with your own.
1. Launch task in VSCode Debugger: `compose dev up`
2. SSH into semaphore contianer: `docker exec -it semaphore-semaphore-dev-main bash`
3. Navigate and run command from inside the container as needed
4. Database can be access using pgAdmin container via: `http://localhost:8888/`. For first time login, you will need to register the database in pgAdmin (see screenshots below)
 hostname: `host.docker.internal`, username: `root`, password: `root`
 ![Register DB to pgAdmin](https://user-images.githubusercontent.com/7061990/268778360-2b92cdc0-19dd-48ae-853c-c52876f747d3.png)
 ![Enter your database credentials to register to pgAdmin](https://user-images.githubusercontent.com/7061990/268778380-36b907c7-da08-4ba8-a232-0c7646dfbb82.png)
5. To stop the containers: Stop the debugger (or Shift + F5)
***If you have problem connecting to the container postgres from semaphore-main container, try changing the environment variable `DB_LOCATION_STRING` value to `postgresql+psycopg2://root:thisisapassword\@host.docker.internal:5432/semaphore-db` (replace `localhost` with `host.docker.internal`). Similarly, use `host.docker.internal` as database hostname to login to the database from `pgAdmin`.

#### Using pgAdmin container:
Login:
username: `admin@admin.com`
password: `root`

## Running Semaphore
Currently, you can run Semaphore through the make_and_save_prediction and make_prediction methods in the `src/ModelExecution/modelWrapper.py`. Keep in mind the DB will need to be loaded with the proper location mappings for your ingested data. Feel free to reach out to one of the authors for help getting Semaphore up and running.

## Authors
* [@Matthew Kastl](https://github.com/matdenkas) - mkastl@islander.tamucc.edu
* [@Beto Estrada](https://github.com/bestrada33) - bestrada4@islander.tamucc.edu
* [@Anointiyae Beasley](https://github.com/abeasley1722) - anointiyae.beasley@tamucc.edu
* [@Florence Tissot](https://github.com/ccftissot)

