import os
import pyspark
import itertools
from pyspark.sql import SparkSession
from pyspark.sql import types

yellow_schema = types.StructType([
        types.StructField('VendorID', types.IntegerType(), True), 
        types.StructField('tpep_pickup_datetime', types.TimestampType(), True), 
        types.StructField('tpep_dropoff_datetime', types.TimestampType(), True), 
        types.StructField('passenger_count', types.IntegerType(), True), 
        types.StructField('trip_distance', types.DoubleType(), True), 
        types.StructField('RatecodeID', types.IntegerType(), True), 
        types.StructField('store_and_fwd_flag', types.StringType(), True),
        types.StructField('PULocationID', types.IntegerType(), True),
        types.StructField('DOLocationID', types.IntegerType(), True),
        types.StructField('payment_type', types.IntegerType(), True), 
        types.StructField('fare_amount', types.DoubleType(), True), 
        types.StructField('extra', types.DoubleType(), True), 
        types.StructField('mta_tax', types.DoubleType(), True), 
        types.StructField('tip_amount', types.DoubleType(), True), 
        types.StructField('tolls_amount', types.DoubleType(), True),
        types.StructField('improvement_surcharge', types.DoubleType(), True), 
        types.StructField('total_amount', types.DoubleType(), True), 
        types.StructField('congestion_surcharge', types.DoubleType(), True),
        types.StructField('airport_fee', types.DoubleType(), True),
])

green_schema = types.StructType([
        types.StructField('VendorID', types.IntegerType(), True),
        types.StructField('lpep_pickup_datetime', types.TimestampType(), True), 
        types.StructField('lpep_dropoff_datetime', types.TimestampType(), True), 
        types.StructField('store_and_fwd_flag', types.StringType(), True), 
        types.StructField('RatecodeID', types.IntegerType(), True), 
        types.StructField('PULocationID', types.IntegerType(), True), 
        types.StructField('DOLocationID', types.IntegerType(), True), 
        types.StructField('passenger_count', types.IntegerType(), True), 
        types.StructField('trip_distance', types.DoubleType(), True), 
        types.StructField('fare_amount', types.DoubleType(), True), 
        types.StructField('extra', types.DoubleType(), True), 
        types.StructField('mta_tax', types.DoubleType(), True), 
        types.StructField('tip_amount', types.DoubleType(), True), 
        types.StructField('tolls_amount', types.DoubleType(), True), 
        types.StructField('ehail_fee', types.DoubleType(), True), 
        types.StructField('improvement_surcharge', types.DoubleType(), True), 
        types.StructField('total_amount', types.DoubleType(), True), 
        types.StructField('payment_type', types.IntegerType(), True), 
        types.StructField('trip_type', types.LongType(), True), 
        types.StructField('congestion_surcharge', types.DoubleType(), True)
])

fhv_schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True), 
    types.StructField('pickup_datetime', types.TimestampType(), True), 
    types.StructField('dropOff_datetime', types.TimestampType(), True), 
    types.StructField('PULocationID', types.IntegerType(), True), 
    types.StructField('DOLocationID', types.IntegerType(), True), 
    types.StructField('SR_Flag', types.StringType(), True),
    types.StructField('Affiliated_base_number', types.StringType(), True)
])


def spark_submit():
    spark = SparkSession.builder \
        .appName('test') \
        .getOrCreate()

    years = ("2020", "2021", "2022")
    for year, month in itertools.product(years, range(1, 13)):
        print(f"processing data for {year}/{month}")
        
        # Process yellow taxi data
        yellow_input_path = f"data/raw/yellow/{year}/{month:02d}/"
        yellow_output_path = f"data/pq/yellow/{year}/{month:02d}/"
        
        if os.path.exists(yellow_input_path):
            print(f"processing data for {year}/{month} (yellow)")
            df_yellow = spark.read \
                .option("header", "true") \
                .schema(yellow_schema) \
                .csv(yellow_input_path)
            df_yellow \
                .repartition(1) \
                .write.parquet(yellow_output_path, mode="overwrite")
        else:
            print(f"No input data found for {year}/{month} (yellow)")
        
        # Process green taxi data
        green_input_path = f"data/raw/green/{year}/{month:02d}/"
        green_output_path = f"data/pq/green/{year}/{month:02d}/"
        
        if os.path.exists(green_input_path):
            print(f"processing data for {year}/{month} (green)")
            df_green = spark.read \
                .option("header", "true") \
                .schema(green_schema) \
                .csv(green_input_path)
            df_green \
                .repartition(1) \
                .write.parquet(green_output_path, mode="overwrite")
        else:
            print(f"No input data found for {year}/{month} (green)")
        
        # Process fhv taxi data
        fhv_input_path = f"data/raw/fhv/{year}/{month:02d}/"
        fhv_output_path = f"data/pq/fhv/{year}/{month:02d}/"
        
        if os.path.exists(fhv_input_path):
            print(f"processing data for {year}/{month} (yellow)")
            df_fhv = spark.read \
                .option("header", "true") \
                .schema(fhv_schema) \
                .csv(fhv_input_path)
            df_fhv \
                .repartition(1) \
                .write.parquet(fhv_output_path, mode="overwrite")
        else:
            print(f"No input data found for {year}/{month} (fhv)")
        
spark_submit()