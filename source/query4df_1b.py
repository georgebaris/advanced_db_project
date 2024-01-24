from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, count, rank, row_number, min , broadcast
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.functions import avg, count, round, desc, year, sum, mean
from pyspark.sql.types import FloatType
import time
from math import radians, sin, cos, sqrt, atan2
from geopy.distance import geodesic


spark = SparkSession.builder.appName("Q4df_1b").getOrCreate()


# Read the DataFrame from the Parquet file
df_main = spark.read.parquet("hdfs:///user/user/crime_data.parquet")
lapd = spark.read.parquet("hdfs:///user/user/lapd.parquet")

df_main.createOrReplaceTempView("df_main")
lapd.createOrReplaceTempView("lapd")


@udf(FloatType())


def get_distance(lat1, lon1, lat2, lon2):

    lat1, lon1, lat2, lon2 = map(radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    radius = 6371.0
    distance = radius * c
    return distance


start_time41b = time.time()

df_main = df_main.filter((col("LAT") != 0.0) & (col("LON") != 0.0))

df_main_weapons = df_main.filter(col("Weapon Used Cd").isNotNull()).select("DR_NO", "LAT", "LON", "DATE OCC", "AREA")


df_main = df_main.withColumn("area", col("area").cast("long"))
lapd= lapd.withColumn("PREC", col("PREC").cast("long")) 

## join df_main with LAPD
crimes_weapons_lapd = df_main_weapons.join(lapd, (df_main_weapons["AREA"] == lapd["PREC"]), "inner")

# crimes_firearms_lapd.show()


## add column distance
crimes_weapons_lapd = crimes_weapons_lapd.withColumn("DISTANCE",get_distance(col("LAT"), col("LON"), col("Y"), col("X")))

q4_1b = crimes_weapons_lapd.groupBy("DIVISION") \
    .agg(round(avg("DISTANCE"), 3).alias("average distance in km"), count("*").alias("Count")) \
    .orderBy("Count", ascending=False)

q4_1b.show(q4_1b.count())
end_time41b = time.time()

print("Execution time: {} seconds".format(end_time41b - start_time41b))

spark.stop()


