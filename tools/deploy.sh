#! /usr/bin/bash
#
# This file is a file used for automatic deployment to a production 
# or development enviroment. It remakes continers with latest code, updates
# the current cron file and inits the DB
#

# Create deployment logs directory if it doesn't exist
mkdir -p ./logs/deployment

# Set up logging - capture both stdout and stderr
LOG_FILE="./logs/deployment/$(date "+%Y")_$(date "+%m")_deployment.log"
DEPLOY_TAG="$1"

# Redirect all output to both console and log file
exec > >(tee -a "$LOG_FILE") 2>&1

# Log deployment start with timestamp and tag
echo "=== Deployment started at $(date '+%Y-%m-%d %H:%M:%S') with tag: $DEPLOY_TAG ===" | tee -a "$LOG_FILE"

# Lower active containers
docker compose down

# Fetch origin            vvvvvv - This will delete any tags that origin has deleted
git fetch origin --tags --prune --prune-tags

# Checkout correct tag
git checkout "$DEPLOY_TAG"

# Build new images and raise containers
docker compose build
docker compose up -d

# Update the cron file on the VM
python3 tools/init_cron.py -r ./data/dspec -i ./schedule

# initialize the db in the containers
docker exec semaphore-core python3 tools/migrate_db.py 

# Sleep to give HELATHCHECKS time to run
echo "Waiting for containers to initialize and pass health checks..."
sleep 30

# Inspect each container and check if its status is heathy
core_status=$(docker inspect semaphore-core | grep -o '"Status": "healthy"')
api_status=$(docker inspect semaphore-api | grep -o '"Status": "healthy"')
db_status=$(docker inspect semaphore-db | grep -o '"Status": "healthy"')

# Check that each status is healthy. 
healthy_status='"Status": "healthy"'
if [[ $core_status == $healthy_status ]] && [[ $api_status == $healthy_status ]] && [[ $db_status == $healthy_status ]]; then
    echo "✓ All containers healthy"
    echo "=== Deployment completed successfully at $(date '+%Y-%m-%d %H:%M:%S') ==="
    exit 0
else
    echo "✗ Container health check failed:"
    echo "  Core: $core_status"
    echo "  API: $api_status"
    echo "  DB: $db_status"
    echo "=== Deployment failed at $(date '+%Y-%m-%d %H:%M:%S') ==="
    exit 1
fi
