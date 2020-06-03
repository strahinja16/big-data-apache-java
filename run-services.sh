#!/bin/sh

cd ./big-data

head -n 100000 weather-data.csv > weather-ml-data.csv

cd ..

docker-compose up