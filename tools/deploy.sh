#! /usr/bin/bash
#
# This file is a file used for automatic deployment to a production 
# or development enviroment. It remakes continers with latest code, updates
# the current cron file and inits the DB
#


# Lower active containers
docker-compose down

# Fetch origin
git fetch origin --tags $1

# Pull latest code 
git pull

# Build new images and raise containers
docker-compose build
docker-compose up -d

# Update the cron file on the VM
python3 tools/init_cron.py -r ./data/dspec -i ./schedule

# initialize the db in the containers
docker exec semaphore-core python3 tools/migrate_db.py 

# Sleep to give HELATHCHECKS time to run
sleep 10

# Inspect each container and check if its status is heathy
core_status=$(docker inspect semaphore-core | grep -o '"Status": "healthy"')
api_status=$(docker inspect semaphore-api | grep -o '"Status": "healthy"')
db_status=$(docker inspect semaphore-db | grep -o '"Status": "healthy"')

# Check that each status is healthy. 
healthy_status='"Status": "healthy"'
if [[ $core_status == $healthy_status ]] && [[ $api_status == $healthy_status ]] && [[ $db_status == $healthy_status ]]; then
    exit 0
else
    exit 1
fi

