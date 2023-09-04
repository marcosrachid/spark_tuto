# spark_tuto

Tutorial spark

# dockers necessarios

## images

- docker pull postgres
- docker pull mongo

## execution

$ docker run --name spark_tuto_postgres --port 5432:5432 -e POSTGRES_PASSWORD=123456 -d postgres

$ docker run --name spark_tuto_mongo --port 27017:27017 -d mongo

## bash

$ docker exec -it spark_tuto_postgres bash
