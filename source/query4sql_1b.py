from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, count, rank, row_number, min , broadcast
from pyspark.sql.functions import round, desc, sum, mean, udf
from pyspark.sql.types import FloatType
import time
from math import radians, sin, cos, sqrt, atan2

#comment this out if you cant register udf on your system
from udfs import get_distance

spark = SparkSession.builder.appName("Q4sql_1b").getOrCreate()


# Read the DataFrame from the Parquet file
df_main = spark.read.parquet("hdfs:///user/user/crime_data.parquet")
lapd = spark.read.parquet("hdfs:///user/user/lapd.parquet")

df_main.createOrReplaceTempView("df_main")
lapd.createOrReplaceTempView("lapd")


# uncomment this if you want to use the udf and cant register it on your system
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

spark.udf.register("get_distance", get_distance)'''

df_main = df_main.filter((col("LAT") != 0.0) & (col("LON") != 0.0))
df_main = df_main.withColumn("area", col("area").cast("long"))
lapd= lapd.withColumn("PREC", col("PREC").cast("long"))


df_main.createOrReplaceTempView("df_main")
lapd.createOrReplaceTempView("lapd")

start_time41b = time.time()

# PySpark SQL query
query = """
WITH WeaponsData AS (
    SELECT
        dm.AREA,
        dm.LAT,
        dm.LON,
        dm.`DATE OCC`,
        lapd.DIVISION,
        lapd.Y,
        lapd.X
    FROM df_main dm
    JOIN lapd ON dm.AREA = lapd.PREC
    WHERE dm.`Weapon Used Cd` IS NOT NULL
),
DistanceData AS (
    SELECT
        DIVISION,
        get_distance(LAT, LON, Y, X) AS DISTANCE,
        YEAR(`DATE OCC`) AS Year
    FROM WeaponsData
)
SELECT
    DIVISION,
    ROUND(MEAN(DISTANCE), 3) AS `average distance in km`,
    COUNT(*) AS Count
FROM DistanceData
GROUP BY DIVISION
ORDER BY Count DESC
"""

# Execute the query
q4_1b = spark.sql(query)
q4_1b.show(q4_1b.count())

end_time41b = time.time()

print("Execution Time: ", end_time41b - start_time41b, "seconds")


spark.stop()
