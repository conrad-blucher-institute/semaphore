# Semaphore

## Description

Semaphore runs AI models saved as H5 files with thier inputs sepecified in a DSPEC JSON.  Semaphore is in the process of being constructed. At the end of its development it should be able to save its work in a database, supply information through an API, and run models automatically and consistantly.

## Deployment

    1. Edit construct env.dict.
        a. DB_LOCATION_STRING Maps your database file locations.or sqlite it will look like this "sqlite:///{YOUR FILE PATH}{your db name}". You do not already need a db in place, it will create it at the location when run for the first time.
        b. DSPEC_FOLDER_PATH & MODEL_FOLDER_PATH can either be relative paths from working directory(default) or you can overide with absolute paths.
    2. requirments.txt (tenserflow CPU build only)
    
## Authors

* [@Matthew Kastl](https://github.com/matdenkas)



