import os
import subprocess

from prefect import flow, task

@task(name="Extract Data From Web", log_prints=True, tags=['extract'])
def extract_data():
    # Execute shell script
    subprocess.run(["./download_data.sh"])

@task(name="Data Processing", log_prints=True)
def spark_processing():
    subprocess.run(["./spark_sh"])
    
@task(name="Write to GCS", log_prints=True)
def upload_gcs():
    data_path = "data/*"
    os.system(f"gsutil -m cp -r {data_path} gs://ny_taxi_data_lake/data/")
    
    
@flow()
def elt_web_gcs():
    """Main ELT Function"""
    extract_data()
    spark_processing()
    upload_gcs()