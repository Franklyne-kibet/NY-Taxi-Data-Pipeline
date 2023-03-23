import os
import pandas as pd
import itertools

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(name="Extract Data From Web", log_prints=True, tags=['extract'])
def extract_data(dataset_url: str, color:str, year:int, dataset_file:str):
    csv_path = f"data/raw/{color}/{year}/{dataset_file}.csv"
    
    df = pd.read_parquet(dataset_url)
    df.to_csv(csv_path, index=False)
    
    return csv_path
@task(name="yellow_schema", log_prints=True)
def data_schema():
    pass

@task(name="Data Transformation", log_prints=True)
def transform_data(csv_path:str, yellow_schema:str,color: str, year: int,month:int) -> pd.DataFrame:
    output_path = f"data/pq/{color}/{year}/{month}/"
       
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('trips') \
        .getOrCreate()
    
    df_yellow_trips = spark.read \
        .option("header","true") \
        .schema(yellow_schema) \
        .csv(csv_path) \

    df_yellow_trips \
        .repartition(4) \
        .write.parquet(output_path, mode='overwrite')
    
    return output_path

@task(name="Write to GCS", log_prints=True)
def upload_to_gcs(output_path:str):
    os.system(f"gsutil -m cp -r {output_path} gs://ny_taxi_data_lake/")

@task(name="Write to BigQuery", log_prints=True)
def upload_to_bq():    
    os.system(f"bq load \
        --source_format=PARQUET \
        de-project-franklyne:trips_data_all.tests \
        gs://ny_taxi_data_lake/data/color/year/month/*.parquet ")
       
@flow()
def etl_web_gcs_bq(year: int, month:int, color: str):
    """Main ETL function"""
    color = "yellow"
    year = 2020
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"
    
    data = extract_data(dataset_url,color,year,dataset_file)
    schema = data_schema()
    clean_data = transform_data(data, schema,color, year,month)
    upload_to_gcs(clean_data)
    upload_to_bq()

@flow()
def etl_parent_flow(
    months:list[int] = list(range(1,13)), years: list[int]= [2020,2021,2022], color:str ="yellow"
):
    for month,year in itertools.product(years, months):
        etl_web_gcs_bq(year,month,color)

if __name__  == '__main__':
    color = "yellow"
    months = list(range(1,13))
    years = [2020,2021,2022]
    etl_parent_flow(months,years,color)