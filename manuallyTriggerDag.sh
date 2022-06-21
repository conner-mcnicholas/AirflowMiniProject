#!/bin/sh
export DOCKCONT=$(eval sudo docker ps | grep start | cut -f1 -d' ')
echo 'Manually triggering dag run'
sudo docker exec $DOCKCONT airflow dags trigger marketvol
echo 'Copying files created in docker container data directory to ./data'
sudo docker cp $DOCKCONT:/tmp/data/ ~/AirflowMiniProject/
echo 'Latest results: '
export DATELS=$(eval ls -lh ./data/ | cut -f9 -d' ' | tail -1)
cat ./data/$DATELS/daily_trade_volumes.txt
