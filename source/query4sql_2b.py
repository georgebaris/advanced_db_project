from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, count, rank, row_number, min , broadcast
from pyspark.sql.window import Window
from pyspark.sql.functions import round, desc, sum, mean, udf
from pyspark.sql.types import FloatType
import time
from math import radians, sin, cos, sqrt, atan2

#comment this out if you cant register udf on your system
from udfs import get_distance

spark = SparkSession.builder.appName("Q4sql_2b").getOrCreate()

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

spark.udf.register("get_distance", get_distance)
'''

df_main = df_main.filter((col("LAT") != 0.0) & (col("LON") != 0.0))

df_main.createOrReplaceTempView("df_main")
lapd.createOrReplaceTempView("lapd")
start_time42b = time.time()

query = """
WITH FilteredMain AS (
    SELECT DR_NO, LAT, LON, `DATE OCC`, AREA
    FROM df_main
    WHERE `Weapon Used Cd` IS NOT NULL
),
CrossJoined AS (
    SELECT fm.DR_NO, fm.LAT, fm.LON, fm.`DATE OCC`, fm.AREA, l.Y, l.X, l.DIVISION
    FROM FilteredMain fm
    CROSS JOIN lapd l
),
Distances AS (
    SELECT *, get_distance(LAT, LON, Y, X) AS DISTANCE,
           ROW_NUMBER() OVER (PARTITION BY DR_NO ORDER BY get_distance(LAT, LON, Y, X)) AS rank
    FROM CrossJoined
),
ClosestPrecincts AS (
    SELECT DR_NO, `DATE OCC`, AREA, DIVISION, DISTANCE
    FROM Distances
    WHERE rank = 1
)
SELECT DIVISION,
       ROUND(MEAN(DISTANCE), 3) AS `average distance in km`,
       COUNT(*) AS Count
FROM ClosestPrecincts
GROUP BY DIVISION
ORDER BY Count DESC
"""

result = spark.sql(query)
result.show(result.count())

end_time42b = time.time()
print("Execution Time: ", end_time42b - start_time42b, "seconds")


spark.stop()









