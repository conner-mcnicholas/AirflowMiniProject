\#!/bin/sh
export DOCKCONT=$(eval sudo docker ps | grep start | cut -f1 -d' ')
echo 'Manually triggering dag run'
sudo docker exec $DOCKCONT airflow dags trigger marketvol
echo 'Copying files created in docker container data directory to ./data'
sudo docker cp $DOCKCONT:/tmp/data/ ~/AirflowMiniProject/
export DATELS=$(eval ls -1 ./data/ | tail -1)
echo 'Resulting analytic query results for latest run ('${DATELS}'):'
cat ./data/$DATELS/daily_trade_volumes.txt
