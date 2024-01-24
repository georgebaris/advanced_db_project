from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, count, rank, row_number, min , broadcast
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.functions import avg, count, round, desc, year, sum, mean
from pyspark.sql.types import FloatType
import time
from math import radians, sin, cos, sqrt, atan2
from geopy.distance import geodesic

spark = SparkSession.builder.appName("Q4sql_2a").getOrCreate()


# Read the DataFrame from the Parquet file
df_main = spark.read.parquet("hdfs:///user/user/crime_data.parquet")
lapd = spark.read.parquet("hdfs:///user/user/lapd.parquet")

df_main = df_main.filter((col("LAT") != 0.0) & (col("LON") != 0.0))

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

spark.udf.register("get_distance", get_distance)


start_time42a = time.time()

spark.sql("""
    WITH FilteredData AS (
        SELECT
            DR_NO, LAT, LON, `DATE OCC`, AREA
        FROM df_main
        WHERE CAST(`Weapon Used Cd` / 100 AS INT) = 1
    ),
    CrossJoined AS (
        SELECT
            f.DR_NO, f.LAT, f.LON, f.`DATE OCC`, f.AREA,
            l.Y, l.X, l.DIVISION
        FROM FilteredData f
        CROSS JOIN lapd l
    ),
    Distances AS (
        SELECT
            *,
            get_distance(LAT, LON, Y, X) AS DISTANCE,
            ROW_NUMBER() OVER (PARTITION BY DR_NO ORDER BY get_distance(LAT, LON, Y, X)) AS rank
        FROM CrossJoined
    ),
    ClosestPrecincts AS (
        SELECT
            DR_NO, `DATE OCC`, AREA, DIVISION, DISTANCE
        FROM Distances
        WHERE rank = 1
    )
    SELECT
        YEAR(`DATE OCC`) AS Year,
        ROUND(MEAN(DISTANCE), 3) AS `average distance in km`,
        COUNT(*) AS Count
    FROM ClosestPrecincts
    GROUP BY YEAR(`DATE OCC`)
    ORDER BY YEAR(`DATE OCC`)
""").show()

end_time42a = time.time()
print("Execution Time: ", end_time42a - start_time42a, "seconds")


spark.stop()


