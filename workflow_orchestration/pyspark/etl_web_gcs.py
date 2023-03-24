import os
import pandas as pd
import subprocess

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(name="Extract Data From Web", log_prints=True, tags=['extract'])
def extract_data():
    download_script = os.path.join(os.getcwd(), 'pyspark', 'download_data.sh')
    subprocess.run(download_script, shell=True, executable='/bin/bash')
    
@task(name="Data Processing", log_prints=True)
def spark_processing():
    spark_submit = os.path.join(os.getcwd(), 'pyspark', 'spark.sh')
    subprocess.run([spark_submit], shell=True)

@task(name="Write to GCS", log_prints=True)
def upload_gcs():
    data_path = "data/"
    os.system(f"gsutil -m cp -r {data_path} gs://ny_taxi_data_lake/data/")

@task (name="Remove Data", log_prints=True)
def remove_data():
    data_path = "data/" 
    os.system(f"rm -rf {data_path}") 
    
@flow()
def etl_web_gcs():
    """Main ELT Function"""
    extract_data()
    spark_processing()
    upload_gcs()
    remove_data()

if __name__  == '__main__':
    etl_web_gcs()