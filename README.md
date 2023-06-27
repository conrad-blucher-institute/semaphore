# Semaphore

## Description

Semaphore runs AI models saved as H5 files with thier inputs sepecified in a DSPEC JSON.  Semaphore is in the process of being constructed. At the end of its development it should be able to save its work in a database, supply information through an API, and run models automatically and consistantly.

## Deployment

    1. Edit src/PersistentStorage/.env DB_LOCATION_STRING to map to your database file location. For sqlite it will look like this "sqlite:///{YOUR FILE PATH}{your db name}". You do not already need a db in place, it will create it at the location when run for the first time.
    2. requirments.txt (tenserflow CPU build only)
## Authors

* [@Matthew Kastl](https://github.com/matdenkas)



