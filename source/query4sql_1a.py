from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, count, rank, row_number, min , broadcast
from pyspark.sql.functions import round, desc, sum, mean, udf
from pyspark.sql.types import FloatType
import time
from math import radians, sin, cos, sqrt, atan2

#comment this out if you cant register udf on your system
from udfs import get_distance

spark = SparkSession.builder.appName("Q4sql_1a").getOrCreate()

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

start_time41a = time.time()


query = """
SELECT
    YEAR(m.`DATE OCC`) as Year,
    ROUND(MEAN(get_distance(m.LAT, m.LON, l.Y, l.X)), 3) as `average distance in km`,
    COUNT(*) as Count
FROM df_main m
JOIN lapd l ON m.AREA = l.PREC
WHERE m.`Weapon Used Cd` LIKE '1%'
GROUP BY YEAR(m.`DATE OCC`)
ORDER BY YEAR(m.`DATE OCC`)
"""

q4_1a = spark.sql(query)
q4_1a.show(q4_1a.count())

end_time41a = time.time()

print("Execution Time: ", end_time41a - start_time41a, "seconds")

spark.stop()








