#! /usr/bin/bash
docker-compose down
git pull
docker-compose build
docker-compose up -d
python3 tools/init_cron.py 
docker exec semaphore-core python3 tools/init_db.py 