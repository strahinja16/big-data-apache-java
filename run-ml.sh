#!/bin/sh

docker exec -it namenode hdfs dfs -rm -r /big-data-weather/ml-model

docker rmi big-data_ml-batch:latest

docker-compose -f docker-compose-ml-batch.yaml up --build