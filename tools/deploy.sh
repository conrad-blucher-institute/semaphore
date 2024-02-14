#! /usr/bin/bash
#
# This file is a file used for automatic deployment to a production 
# or development enviroment. It remakes continers with latest code, updates
# the current cron file and inits the DB
#


# Lower active containers
docker-compose down

# Pull latest code 
git pull

# Build new images and raise containers
docker-compose build
docker-compose up -d

# Update the cron file on the VM
python3 tools/init_cron.py 

# initialize the db in the containers
docker exec semaphore-core python3 tools/init_db.py 