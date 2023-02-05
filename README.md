# Advanced DataBases Project NTUA 2022-2023

## Overall Description

In this task we are required to process volume data using the Apache Spark. 
The data is about taxi trip records in New York City. The trip records include fields related to the dates/times of pickup and drop-off times and times, pick-up and drop-off locations, trip distances, detailed fares, fare types, fare types, payment types and number of passengers reported by the driver. 
The data used in the attached data sets were collected and provided to the New York Taxi and Limousine Commission (TLC) by providers technology providers. The data (by month) and description are open and can be found at https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page.

We used Spark version 3.3.1 with HDFS as the system file storage. Also, we used the DataFrame API and the RDD API. Python3.8 is also necessary.
The Yellow Taxi Trip Records data required for our project are for the months 
January to June 2022 (in compressed parquet format). The exact description of the fields of the data can be found here:
https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

In addition, we also used the file, https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv, which links the field LocationID with a verbal representation of the information in suburbs and zones of the city.

## Queries Description

**Q1:**   Find the route with the biggest tip in March and point arrival point is "Battery Park". 

**Q2:**   Find, for each month, the route with the highest toll. Ignore zero amounts. 

**Q3:**   Find, per 15 days, the average distance and cost for all the routes with a different departure point from the arrival point. 

**Q4:** Find the top 3 peak hours per day of the week, meaning the hours (e.g., 7-8am, 3-4pm, etc.) of the day with the highest number of the day of the week with the highest number of passengers in a taxi journey. The calculation applies to all months.

**Q5:** Find the top five (top 5) days per month on which rides were the highest percentage of tips. For example, if a ride cost $10 (fare_amount) and the tip was $5, the percentage is 50%.

In detail, **the questions of the assignment** are as follows:

1. Install the Spark & HDFS execution platform and create two RDDs and two DataFrames from the original data (taxi trips & zone lookups).

2. Run Q1, Q2 using the DataFrame/SQL API. We want the results and execution times of the query using 1 and 2 workers (and all the available CPUs ). To get the correct execution times, make sure tocollect the result of each query (or write to hdfs-disk.

3. Run Q3 using the DataFrame/SQL API and the RDD API. We want the results and execution times of the query using 1 and 2 workers. 

4. Run Q4, Q5 using the DataFrame/SQL API. We want the results and execution times of the query using 1 and 2 workers.

### What you'll find in this repository:

* `all_queries.py` : run with `spark-submit all_queries.py` command, which executes all the queries at once.

* `query_choice.py` : gives the user the option to run only one of the queries via user input and we run it with `python.3.8 query_choice.py` command.

* `03118117_03118165.pdf` : answers for all the questions of the project
