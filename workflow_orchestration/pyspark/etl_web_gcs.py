import os
import pandas as pd
import itertools
import subprocess

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(name="Extract Data From Web", log_prints=True, tags=['extract'])
def extract_data():
    bash_script = '''
        set -e 

        TAXI_TYPES=("yellow" "green" "fhv") 
        YEARS=("2019" "2020" "2021" "2022") 
        URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data" 

        for TAXI_TYPE in "${TAXI_TYPES[@]}"; do 
            for YEAR in "${YEARS[@]}"; do 
                for MONTH in {1..12}; do 
                    FMONTH=`printf "%02d" ${MONTH}` 
                    URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet" 
                    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}" 
                    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet" 
                    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}" 
                    CSV_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv"
                    
                    echo "downloading ${URL} to ${LOCAL_PATH}" 
                    mkdir -p ${LOCAL_PREFIX} 
                    wget ${URL} -O ${LOCAL_PATH} 
                    
                    echo "converting ${LOCAL_PATH} to CSV format" 
                    parquet-tools csv ${LOCAL_PATH} | csvformat -T > ${LOCAL_PREFIX}/${CSV_FILE}
                    
                    echo "removing ${LOCAL_PATH}" 
                    rm ${LOCAL_PATH}
                done 
            done 
        done
    '''

    subprocess.run(bash_script, shell=True)
    
@task(name="Data Processing", log_prints=True)
def spark_processing():
    pass

@task(name="Write to GCS", log_prints=True)
def upload_gcs():
    # data_path = "data/*"
    # os.system(f"gsutil -m cp -r {data_path} gs://ny_taxi_data_lake/data/")
    pass
    
@flow()
def elt_web_gcs():
    """Main ELT Function"""
    extract_data()
    spark_processing()
    upload_gcs()

if __name__  == '__main__':
    elt_web_gcs()