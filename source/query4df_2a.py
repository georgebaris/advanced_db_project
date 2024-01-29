from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, count, rank, row_number, min , broadcast
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, round, desc, udf
from pyspark.sql.types import FloatType
import time
from math import radians, sin, cos, sqrt, atan2

#comment this out if you cant register udf on your system
from udfs import get_distance

spark = SparkSession.builder.appName("Q4df_2a").getOrCreate()

# Read the DataFrame from the Parquet file
df_main = spark.read.parquet("hdfs:///user/user/crime_data.parquet")
lapd = spark.read.parquet("hdfs:///user/user/lapd.parquet")

df_main.createOrReplaceTempView("df_main")
lapd.createOrReplaceTempView("lapd")


# uncomment this if you want to use the udf and cant register it on your sysem
'''@udf(FloatType())
def get_distance(lat1, lon1, lat2, lon2):

    lat1, lon1, lat2, lon2 = map(radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    radius = 6371.0
    distance = radius * c
    return distance
'''
start_time42a = time.time()
df_main = df_main.filter((col("LAT") != 0.0) & (col("LON") != 0.0))
# Select relevant columns and perform a cross join
df_main_firearms = df_main.filter((col("Weapon Used Cd") / 100).cast("int") == 1).select("DR_NO", "LAT", "LON", "DATE OCC", "AREA")
cross_joined = df_main_firearms.crossJoin(lapd)

with_distances = cross_joined.withColumn("DISTANCE", get_distance(col("LAT"), col("LON"), col("Y"), col("X")))

windowSpec = Window.partitionBy(cross_joined["DR_NO"]).orderBy("DISTANCE")

ranked = with_distances.withColumn("rank", row_number().over(windowSpec))

# Filter to keep only the closest precinct (rank = 1) for each row in the main DataFrame
closest_precincts = ranked.filter(col("rank") == 1).drop("rank")

# Now, closest_precincts DataFrame has each row from df_main along with the details of its closest precinct
q4_2a = closest_precincts.groupBy(year(closest_precincts["DATE OCC"]).alias("Year")) \
    .agg(round(avg("DISTANCE"), 3).alias("average distance in km"), count("*").alias("Count")) \
    .orderBy("Year", ascending=True)

q4_2a.show(q4_2a.count())
end_time42a = time.time()

# Calculate and print the execution time
print("Execution Time: ", end_time42a - start_time42a, "seconds")

spark.stop()
