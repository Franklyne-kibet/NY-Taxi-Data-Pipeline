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
        types.StructField('ehail_fee', types.DoubleType(), True),
        types.StructField('improvement_surcharge', types.DoubleType(), True), 
        types.StructField('total_amount', types.DoubleType(), True), 
        types.StructField('congestion_surcharge', types.DoubleType(), True)
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

    taxi_types = ("yellow", "green", "fhv")
    years = ("2019", "2020", "2021", "2022")

    for yellow, year, month in itertools.product(taxi_types, years, range(1, 13)):
                print(f"processing data for {year}/{month} ({yellow})")
                
                input_path = f"data/raw/{yellow}/{year}/{month:02d}/"
                output_path = f"data/pq/{yellow}/{year}/{month:02d}/"
                
                df_taxi = spark.read \
                    .option("header", "true") \
                    .schema(yellow_schema) \
                    .csv(input_path)
                    
                df_taxi \
                    .repartition(4) \
                    .write.parquet(output_path, mode="overwrite")
                    
    for green, year, month in itertools.product(taxi_types, years, range(1, 13)):
                print(f"processing data for {year}/{month} ({green})")
                
                input_path = f"data/raw/{green}/{year}/{month:02d}/"
                output_path = f"data/pq/{green}/{year}/{month:02d}/"
                
                df_green = spark.read \
                    .option("header", "true") \
                    .schema(green_schema) \
                    .csv(input_path)
                    
                df_green \
                    .repartition(4) \
                    .write.parquet(output_path, mode="overwrite")

    for fhv, year, month in itertools.product(taxi_types, years, range(1, 13)):
                print(f"processing data for {year}/{month} ({green})")
                input_path = f"data/raw/{fhv}/{year}/{month:02d}/"
                output_path = f"data/pq/{fhv}/{year}/{month:02d}/"
                
                df_fhv = spark.read \
                    .option("header", "true") \
                    .schema(fhv_schema) \
                    .csv(input_path)
                    
                df_fhv \
                    .repartition(4) \
                    .write.parquet(output_path, mode="overwrite")
spark_submit()