# BigData course projects
Project 1: Spark SQL queries (with UDF)

Project 2: Spark streaming + Kafka


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

## Before running any project
```
docker-compose up
```
- Wait for all containers to start then run the next command to place datasets on hdfs

```
./place-datasets-to-hdfs.sh
```

### Setup Mongo

Grant user access to the Weather db used by streaming consumer.

```
docker exec -it mongo mongo -u "root" -p "root1234"
```
```
use admin
```
```
db.updateUser( "root", { roles : [{ role : "root", db : "admin"  },{ role : "readWrite", db: "weather"  }]})
```
```
exit
```

## Running the Project 1
Spark SQL application.
Queries:
- GetCountriesSortedByMostSevereWinter - [Weather dataset] Reads the winter reports for given year and returns a sorted list of countries by descending count of severe reports.
- GetSeverityStatisticsPerEventTypeForCityInLastTenYears - [Weather dataset] Gets the min, max, avg, and stddev values for each event type in given city for the last ten years.
- GetCityWithMinAndMaxBrokenVehiclesInFiveYears - [Traffic dataset] Returns the two cities with the most and the least broken vehicles in last five years.


Run Spark submit application

```
docker-compose -f docker-compose-submit.yaml up --build
```

## Running the Project 2
Spark Streaming application.


Run Spark producer and consumer applications

```
docker-compose -f docker-compose-streaming.yaml up --build
```