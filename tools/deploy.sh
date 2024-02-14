#! /usr/bin/bash
docker-compose down
git pull
docker-compose build
docker-compose up -d
python3 ./tools/init_cron.py
python4 ./tools/init_db.py