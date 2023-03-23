-- FHV Trips Data
CREATE OR REPLACE EXTERNAL TABLE `de-project-franklyne.trips_data_all.external_fhv_tripdata`
OPTIONS
(
    format = 'parquet',
    uris = [
        'gs://ny_taxi_data_lake/data/raw/fhv/2019/*.parquet',
        'gs://ny_taxi_data_lake/data/raw/fhv/2020/*.parquet',
        'gs://ny_taxi_data_lake/data/raw/fhv/2021/*.parquet',
        'gs://ny_taxi_data_lake/data/raw/fhv/2022/*.parquet'
    ]
);

CREATE OR REPLACE TABLE `de-project-franklyne.trips_data_all.fhv_tripdata`
AS
SELECT * FROM `de-project-franklyne.trips_data_all.external_fhv_tripdata`;

-- Yellow Trips Data
CREATE OR REPLACE EXTERNAL TABLE `de-project-franklyne.trips_data_all.external_yellow_tripdata`
OPTIONS
(
    format = 'parquet',
    uris = [
        'gs://ny_taxi_data_lake/data/raw/yellow/2019/*.parquet',
        'gs://ny_taxi_data_lake/data/raw/yellow/2020/*.parquet',
        'gs://ny_taxi_data_lake/data/raw/yellow/2021/*.parquet',
        'gs://ny_taxi_data_lake/data/raw/yellow/2022/*.parquet'
    ]
);

CREATE OR REPLACE TABLE `de-project-franklyne.trips_data_all.yellow_tripdata`
AS
SELECT * FROM `de-project-franklyne.trips_data_all.external_yellow_tripdata`;

-- Green Trips Data
CREATE OR REPLACE EXTERNAL TABLE `de-project-franklyne.trips_data_all.external_green_tripdata`
OPTIONS
(
    format = 'parquet',
    uris = [
        'gs://ny_taxi_data_lake/data/raw/green/2019/*.parquet',
        'gs://ny_taxi_data_lake/data/raw/green/2020/*.parquet',
        'gs://ny_taxi_data_lake/data/raw/green/2021/*.parquet',
        'gs://ny_taxi_data_lake/data/raw/green/2022/*.parquet'
    ]
);

CREATE OR REPLACE TABLE `de-project-franklyne.trips_data_all.green_tripdata`
AS
SELECT * FROM `de-project-franklyne.trips_data_all.external_green_tripdata`;