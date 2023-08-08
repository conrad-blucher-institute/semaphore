# Semaphore

## Description

Semaphore runs AI models saved as H5 files with thier inputs sepecified in a DSPEC JSON.  Semaphore is in the process of being constructed. At the end of its development it should be able to save its work in a database, supply information through an API, and run models automatically and consistantly.

## Deployment

    1. Edit construct `env.dist`.
        a. DB_LOCATION_STRING Maps your database file locations.or sqlite it will look like this "sqlite:///{YOUR FILE PATH}{your db name}". You do not already need a db in place, it will create it at the location when run for the first time.
        b. DSPEC_FOLDER_PATH & MODEL_FOLDER_PATH can either be relative paths from working directory(default) or you can overide with absolute paths.
    2. `requirements.txt`` (tenserflow CPU build only)

____
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
4. Database can be access using: `http://localhost:8888/`, username: `root`, password: `thisisapassword`
5. To stop the containers: Stop the debugger (or Shift + F5)
***If you have problem connecting to the container postgres from semaphore-main container, try changing the environment variable `DB_LOCATION_STRING` value to `postgresql+psycopg2://root:thisisapassword\@host.docker.internal:5432/semaphore-db` (replace `localhost` with `host.docker.internal`)
___
## Authors

* [@Matthew Kastl](https://github.com/matdenkas)



