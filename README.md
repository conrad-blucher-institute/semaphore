# Semaphore

## Description

Semaphore is a student-built and maintained Python application that semi-automates the process of operationalizing AI models. Semaphore downloads data, runs models, post-process outputs, and saves outputs. Semaphore currently takes model data in the form of .H5 files and input specifications and a DSPEC JSON. Examples of both are included in this repository.

## Dependency
Right now the only external dependency that is not included in the requirements file, is some version of SQLite as this is the only Database system currently supported.
[SQLite Download Page](https://www.sqlite.org/download.html)
    
## Deployment

1. Construct env.dict    
    - Create the .env file at the root directory of Semaphore, and copy the text from env.dict into it.
    - Assign DB_LOCATION_STRING. It maps your database file locations. It Should look like this "sqlite:///{YOUR FILE PATH}{your db file name}". You do not already need a DB file in place, it will create it at the location when run for the first time.
    - (Optional) DSPEC_FOLDER_PATH & MODEL_FOLDER_PATH can either be relative paths from the working directory(default) or you can override with absolute paths.
2. Create virtual environment & install required Python dependencies
```python
python3 -m venv ~/venv/{venv name}
source ~/venv/{venv name}/Scripts/activate
pip install -r requirements.txt
```
(*NOTE:: depends on the python version, you may want to use `source ~/venv/{venv name}/bin/activate)` instead)

(**NOTE:: TensorFlow currently CPU build only.)

## Running Semaphore
Currently, you can run Semaphore through the make_and_save_prediction and make_prediction methods in the `src/ModelExecution/modelWrapper.py`. Keep in mind the DB will need to be loaded with the proper location mappings for your ingested data. Feel free to reach out to one of the authors for help getting Semaphore up and running.

## Authors
* [@Matthew Kastl](https://github.com/matdenkas) - mkastl@islander.tamucc.edu
* [@Beto Estrada](https://github.com/bestrada33) - bestrada4@islander.tamucc.edu
* [@Anointiyae Beasley](https://github.com/abeasley1722) - anointiyae.beasley@tamucc.edu
* [@Florence Tissot](https://github.com/ccftissot)

