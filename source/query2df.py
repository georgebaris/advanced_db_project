from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, count, rank
from pyspark.sql.functions import udf, StringType
import time

#comment this out if you can't register udfs on your cluster
from udfs import get_day_segment

spark = SparkSession.builder.appName("Q2df").getOrCreate()


# Read the DataFrame from the Parquet file
df_main = spark.read.parquet("hdfs:///user/user/crime_data.parquet")


# create view
df_main.createOrReplaceTempView("df_main")

start_time = time.time()
'''
#uncomment this if you can't register udfs on your cluster 
#and run the function as day_segment_udf()


def get_day_segment(time_str):
        # Extract hour part and convert to integer
            hour = int(time_str[:2])

            if 5 <= hour < 12:
                 return 'Morning'
            elif 12 <= hour < 17:
                 return 'Afternoon'
            elif 17 <= hour < 21:
                 return 'Evening'
            else:
                 return 'Night'
day_segment_udf = udf(get_day_segment, StringType())
'''


df_filtered = df_main.filter(col("Premis Desc") == "STREET")
df_segmented = df_filtered.withColumn('Day_Segment', get_day_segment(col('TIME OCC').substr(0, 2)))
crime_count_by_segment = df_segmented.groupBy('Day_Segment').agg(count('*').alias('Crime_Count'))
result = crime_count_by_segment.orderBy(col('Crime_Count').desc())

result.show()

end_time = time.time()
print("Execution time: {} seconds".format(end_time - start_time))
spark.stop()
