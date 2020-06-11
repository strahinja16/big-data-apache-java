#!/bin/sh

docker rmi big-data_ml-streaming:latest
docker rmi big-data_ml-streaming-producer:latest

docker-compose -f docker-compose-ml-streaming.yaml up --build --force-recreate