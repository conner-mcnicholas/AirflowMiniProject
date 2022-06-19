#!/bin/sh
echo 'Copying files created in docker container data directory to ./data'
export DOCKCONT=$(eval sudo docker ps | grep start | cut -f1 -d' ')
sudo docker cp $DOCKCONT:/tmp/data/ ~/AirflowMiniProject/
