from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col
from operator import add
import time


#comment this out if you can't register udfs on your cluster
from udfs import get_day_segment

spark = SparkSession.builder.appName("Q2RDD").getOrCreate()

df_main = spark.read.parquet("hdfs:///user/user/crime_data.parquet")


'''
# uncomment this if you can't register udfs on your system
def get_day_segment(record):
    try:
        hour = int(record['TIME OCC'][:2])
        if 5 <= hour < 12:
            return 'Morning'
        elif 12 <= hour < 17:
            return 'Afternoon'
        elif 17 <= hour < 21:
            return 'Evening'
        else:
            return 'Night'
    except ValueError:
        return 'Unknown'
'''

start_2rdd = time.time()

df_filtered = df_main.filter(col("Premis Desc") == "STREET")

# Convert to RDD
rdd_filtered = df_filtered.rdd

# Apply the segmentation function and map to (segment, 1)
segments = rdd_filtered.map(lambda x: (get_day_segment(x), 1))

# Reduce by key to count occurrences
crime_count_by_segment = segments.reduceByKey(add)

result = crime_count_by_segment.collect()
result.sort(key=lambda x: x[1], reverse=True)



# show the results as a df view
# transform into rows for the df
rows = [Row(Segment=segment, Count=count) for segment, count in result]

# define the result df by Rows
result_df = spark.createDataFrame(rows)

result_df.show()

end_2rdd = time.time()

print(f"Execution time for Query 2 RDD API:{end_2rdd - start_2rdd}")
spark.stop()