docker kill ml-streaming
docker rm ml-streaming
docker rmi big-data_ml-streaming:latest
docker-compose -f ../docker-compose-4.yaml up -d