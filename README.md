# Useful info

## Prequisites
I'm assuming you have git installed in order to pull this repo and docker/docker-compose installed in order to setup the environment

The versions I used are attached below, but any recent version of docker/docker-compose should work.  
```
docker --version
Docker version 18.03.1-ce, build 9ee9f40

docker-compose --version
docker-compose version 1.21.1, build 5a3f1a3
```

## Running tests
The easiest way to run the tests is running them inside a docker container, run the shell script:
```
./test_dockerized.sh
```
This will download a python3 docker container, install dependencies in it, and then run the tests

## Running the data pipeline
```
docker-compose build
docker-compose up

...
(wait until things start up)
...

go to: localhost:8080 for the airflow web UI

enable the "spothero_challenge" DAG
```

## Info about the data pipeline
- The pipeline is a DAG in Airflow. By default it is not enabled in order to not spam your computer with background tasks. Please enable the spothero_challenge DAG by toggling the "OFF/ON" button in the Airflow UI or it will not run. 
- The pipeline runs every 30 minutes by default.
- The pipeline runs locally (not in a cloud provider). I did this to save time and and money as I did not want to create a separate account with billing info just for this challenge. 
- The output data gets put in the `data` folder. You can see temporary/intermediate results as well as the final report `report.final.csv`.
- The pipeline will skip fetching weather data if the chicago movies dataset ETAG has not changed. This is to prevent hitting the darksky API limit. The other datasets do not have similar caching logic implemented since they process so quickly anyways and there are no API limits to worry about. 


## Workflow image
<img src="/docs/airflow_img.png" alt="Image of the successful airflow workflow"/>
