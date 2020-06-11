# BigData course projects
Project 1: Spark SQL queries (with UDF)

Project 2: Spark streaming + Kafka

Project 3: Spark ML

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
./run-services.sh
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
db.updateUser("root", { roles : [{ role : "root", db : "admin" },{ role : "readWrite", db: "weather" }]})
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

Producer reads the weather dataset and sends the data via Kafka topic, one row per second. 

Consumer reads the data and filters rain events for city Saguache that are stored to Mongo db. Consumer also uses Spark workers to calculate, after each batch, min, max and avg minutes of Saguache's raining events duration. 

Run Spark producer and consumer applications

```
docker-compose -f docker-compose-streaming.yaml up --build
```

## Running the Project 3
Spark ML training application.

Application first filters the weather dataset, removes correlated attributes and transforms the data to discrete numerical values.Then the model is trained in order to predict the severity of weather event based on event type, season of year and geographical latitude and longitude.

```
./run-ml.sh
```

Application will train the model using `weather-ml.csv` in HDFS which has 100.000 records. 

In order for it to use the whole weather dataset:
      
Change `APP_ARGS_CSV_FILE_PATH: /big-data-weather/weather-ml.csv` to `APP_ARGS_CSV_FILE_PATH: /big-data-weather/weather.csv`
 in the `docker-compose-ml-batch.yaml`.
 
 Model's accuracy  is `79%`.     
 
Spark ML streaming application.

```
./run-ml-streaming.sh
```
Running this script will start the producer application which will send the weather data via Kafka topic, row by row. 
ML Streaming Consumer application reads the data and tries to predict the severity of weather event using loaded ml model previously saved by the Spark ML training application.