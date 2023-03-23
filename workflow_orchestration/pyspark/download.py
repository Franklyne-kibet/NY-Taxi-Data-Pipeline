import os

TAXI_TYPES = ["yellow", "green", "fhv"]
YEARS = ["2019", "2020", "2021", "2022"]
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"

for taxi_type in TAXI_TYPES:
    for year in YEARS:
        for month in range(1, 13):
            fmonth = f"{month:02d}"
            
            url = f"{URL_PREFIX}/{taxi_type}_tripdata_{year}-{fmonth}.parquet"
            
            local_prefix = f"data/raw/{taxi_type}/{year}/{fmonth}"
            local_file = f"{taxi_type}_tripdata_{year}_{fmonth}.parquet"
            local_path = os.path.join(local_prefix, local_file)
            
            print(f"downloading {url} to {local_path}")
            os.makedirs(local_prefix, exist_ok=True)
            f"wget {url} -O {local_path}"
