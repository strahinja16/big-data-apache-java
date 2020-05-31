#!/bin/sh

docker exec -it namenode hdfs dfs -test -e /big-data
if [ $? -eq 1 ]
then
  echo "[INFO]: Creating /big-data folder on HDFS"
  docker exec -it namenode hdfs dfs -mkdir /big-data
fi

docker exec -it namenode hdfs dfs -test -e /big-data/data.csv
if [ $? -eq 1 ]
then
  echo "[INFO]: Adding data.csv in the /big-data folder on the HDFS"
  docker exec -it namenode hdfs dfs -put /big-data/data.csv /big-data/data.csv
fi

docker exec -it namenode hdfs dfs -test -e /big-data-wather
if [ $? -eq 1 ]
then
  echo "[INFO]: Creating /big-data-weather folder on HDFS"
  docker exec -it namenode hdfs dfs -mkdir /big-data-weather
fi

docker exec -it namenode hdfs dfs -test -e /big-data-weather/weather.csv
if [ $? -eq 1 ]
then
  echo "[INFO]: Adding weather.csv in the /big-data-weather folder on the HDFS"
  docker exec -it namenode hdfs dfs -put /big-data-weather/weather.csv /big-data-weather/weather.csv
fi
