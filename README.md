# Semaphore

## Description

Semaphore is a student-built and maintained Python application that semi-automates the process of operationalizing AI models. Semaphore downloads data, runs models, post-process outputs, and saves outputs. Semaphore currently takes model data in the form of .H5 files and input specifications and a DSPEC JSON. Examples of both are included in this repository.

## Dependency
    Right now the only external dependency that is not included in the requirements file, is some version of SQLite as this is the only Database system currently supported.
    
## Deployment

    1. Edit construct env.dict.
        a. DB_LOCATION_STRING Maps your database file locations.or sqlite it will look like this "sqlite:///{YOUR FILE PATH}{your db name}". You do not already need a db in place, it will create it at the location when run for the first time. 
        b. DSPEC_FOLDER_PATH & MODEL_FOLDER_PATH can either be relative paths from working directory(default) or you can overide with absolute paths.
    2. requirments.txt (tenserflow currently CPU build only)
    
## Authors

* [@Matthew Kastl](https://github.com/matdenkas)



