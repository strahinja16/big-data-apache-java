# BigData course projects
Project 1: Spark SQL queries (with UDF)

HDFS - One namenode and one datanode.

Spark - Master node and two workers.

Both HDFS and project related services are running in Docker.

## Pre-reqs

- [Docker](https://docs.docker.com/engine/install/ubuntu/)

### Traffic and weather datasets

- [Dataset description link](https://smoosavi.org/datasets/lstw/)
- [Weather dataset download link](https://osu.app.box.com/v/weather-events-dec19)
- [Traffic dataset download link](https://osu.app.box.com/v/traffic-events-dec19)

## Setup


- Clone project
```
git clone git@github.com:strahinja16/big-data-spark-java.git
```
- Create folder for the dataset
```
mkdir big-data
```
- Download the datasets, rename them and place in big-data folder
```
big-data/
    traffic-data.csv
    weather-data.csv
```

## Running the Project 1

```
docker-compose -f docker-compose-1.yaml up
```

- Wait for all containers to start then run the next command to place datasets on hdfs

```
./place-datasets-to-hdfs.sh
```

- Run Spark submit application

```
docker-compose -f docker-compose-submit.yaml up --build
```
