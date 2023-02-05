import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, expr, col, desc, month, asc, row_number, sum
from pyspark.sql.functions import max, lit, dayofmonth, ntile, avg, round, dayofweek, hour
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

query = input("Enter the query (1 to 5) you'd like to execute: ") 
if query not in ["1", "2", "3", "4", "5"]:
    print("Your answer don't match the choices.. aborting..")
    sys.exit()
elif query == "3":
    api = input("Choose between DataFrame/SQL API (as df) or RDD API (as rdd): ")
    if api not in ["df", "rdd"]:
        print("Your answer don't match the choices.. aborting..")
        sys.exit()
else: 
    print("Got it, let's start by creating a Spark session..")

        
spark = SparkSession.builder.appName("OurApp").getOrCreate()
print("\nSpark session created...\n")

print("\nReading taxi_trips parquet files...\n")
taxi_trips_df = spark.read.parquet('hdfs://master:9000/taxi_trips/yellow_tripdata_2022-01.parquet', 
                    'hdfs://master:9000/taxi_trips/yellow_tripdata_2022-02.parquet',
                    'hdfs://master:9000/taxi_trips/yellow_tripdata_2022-03.parquet',
                    'hdfs://master:9000/taxi_trips/yellow_tripdata_2022-04.parquet',
                    'hdfs://master:9000/taxi_trips/yellow_tripdata_2022-05.parquet',
                    'hdfs://master:9000/taxi_trips/yellow_tripdata_2022-06.parquet')

print("\nReading taxi_zones csv file...\n")
taxi_zones_df = spark.read.csv("hdfs://master:9000/taxi_zones/taxi+_zone_lookup.csv")

print("\nDone with reading files...\n")

print("Some preprocessing on the data.../n")

taxi_trips_df = taxi_trips_df.filter(
  (col("tpep_pickup_datetime") >= lit("2022-01-01")) &
  (col("tpep_pickup_datetime") < lit("2022-07-01"))
)

print("\nNow.. creating RDDs..\n")

taxi_trips_rdd = taxi_trips_df.rdd
taxi_zones_rdd = taxi_zones_df.rdd

print("\nDone creating RDDs...\n")
print("\nQuery starting now...\n")

if query == "1":

    start_time_Q = time.time()

    # we make a choice to filter out rows with trip_distance = 0.0
    # you can change it if you just delete line 61

    result = taxi_trips_df.filter((month(col("tpep_pickup_datetime"))) == 3)\
            .filter((col("trip_distance") > 0.0))\
            .join(taxi_zones_df, [taxi_trips_df.DOLocationID == taxi_zones_df._c0, taxi_zones_df._c2 == "Battery Park"])\
            .sort(desc("tip_amount"))\
            .drop("_c0","_c1","_c2","_c3")\
            .first()

    stop_time_Q = time.time()

elif query == "2":

    start_time_Q = time.time()

    result = taxi_trips_df.filter(col("Tolls_amount") > 0)\
            .groupBy(month(col("tpep_pickup_datetime")))\
            .agg(max("Tolls_amount")\
            .alias("max_per_month_toll_amount"))\
            .sort(asc("month(tpep_pickup_datetime)"))\
            .join(taxi_trips_df, [month(col("tpep_pickup_datetime")) == col("month(tpep_pickup_datetime)"), col("Tolls_amount") == col("max_per_month_toll_amount")])\
            .drop("month(tpep_pickup_datetime)","max_per_month_toll_amount")\
            .collect()

    stop_time_Q = time.time()

elif query == "3":
    if api == "df":

        start_time_Q = time.time()

        result = taxi_trips_df.filter(col("PULocationID") != col("DOLocationID")) \
                .withColumn("15_days_intervals", expr("case when day(tpep_pickup_datetime) <= 15 \
                                                    then concat(month(tpep_pickup_datetime),'first') \
                                                    else concat(month(tpep_pickup_datetime),'second') end"))\
                .groupBy("15_days_intervals")\
                .agg(avg("trip_distance"), avg("total_amount"))\
                .orderBy(asc("15_days_intervals"))\
                .collect()

        stop_time_Q = time.time()

    else:

        start_time_Q = time.time()

        result = taxi_trips_rdd.filter(lambda x: x.PULocationID != x.DOLocationID)\
                .map(lambda x: (f"{x.tpep_pickup_datetime.month}-{'first_' if x.tpep_pickup_datetime.day <= 15 else 'second_'}", (x.trip_distance, x.total_amount, 1)))\
                .aggregateByKey((0, 0, 0), 
                        lambda acc, value: (acc[0]+value[0], acc[1]+value[1], acc[2]+1), 
                        lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1], acc1[2]+acc2[2]))\
                .mapValues(lambda x: (x[0]/x[2], x[1]/x[2]))\
                .sortByKey()\
                .collect()

        stop_time_Q = time.time()

elif query == "4":

    start_time_Q = time.time()

    result = taxi_trips_df.groupBy(dayofweek(col("tpep_pickup_datetime")), hour(col("tpep_pickup_datetime")))\
            .agg(sum(col("passenger_count").cast("int")).alias("total_passenger_count"))\
            .withColumn("rank", row_number().over(Window.partitionBy("dayofweek(tpep_pickup_datetime)").orderBy(desc("total_passenger_count"))))\
            .filter(col("rank") <= 3)\
            .sortWithinPartitions(asc("dayofweek(tpep_pickup_datetime)"), asc("rank"))\
            .collect()

    stop_time_Q = time.time()

else:

    start_time_Q = time.time()

    result = taxi_trips_df.groupBy([month(col("tpep_pickup_datetime")), dayofmonth(col("tpep_pickup_datetime"))])\
            .agg(sum("Fare_amount").alias("fare_amount_sum"), sum("Tip_amount").alias("tip_amount_sum"))\
            .withColumn("tip_percentage", col("tip_amount_sum")/col("fare_amount_sum"))\
            .withColumn("rank", row_number().over(Window.partitionBy("month(tpep_pickup_datetime)").orderBy(desc("tip_percentage"))))\
            .filter(col("rank") <= 5)\
            .sort(asc("month(tpep_pickup_datetime)"),asc("rank"))\
            .drop("fare_amount_sum", "tip_amount_sum")\
            .collect()

    stop_time_Q = time.time()

print("\nQuery results are available..\n")
print(result)

time_Q = stop_time_Q - start_time_Q

print("\nTime taken for the query is: ", time_Q)
print()