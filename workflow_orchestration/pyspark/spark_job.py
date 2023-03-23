import pyspark
from pyspark.sql import SparkSession

def spark_submit():
    spark = SparkSession.builder \
        .appName('test') \
        .getOrCreate()

    taxi_types = ("yellow", "green", "fhv")
    years = ("2019", "2020", "2021", "2022")

    for taxi_type in taxi_types:
        for year in years:
            for month in range(1, 13):
                print(f"processing data for {year}/{month} ({taxi_type})")
                
                input_path = f"data/raw/{taxi_type}/{year}/{month:02d}/"
                output_path = f"data/pq/{taxi_type}/{year}/{month:02d}/"
                
                df_taxi = spark.read \
                    .option("header", "true") \
                    .csv(input_path)
                    
                df_taxi \
                    .repartition(4) \
                    .write.parquet(output_path, mode="overwrite")

spark_submit()
