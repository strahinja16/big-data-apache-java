#!/bin/sh

docker exec -it namenode hdfs dfs -rm -r /ml-model

docker-compose -f docker-compose-ml-batch.yaml down
docker rmi big-data_ml-batch:latest

docker-compose -f docker-compose-ml-batch.yaml up --build
