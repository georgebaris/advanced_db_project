from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, count, rank
from pyspark.sql.window import Window
import time


spark = SparkSession.builder.appName("Q1sql").getOrCreate()

# Read the DataFrame from the Parquet file
df_main = spark.read.parquet("hdfs:///user/user/crime_data.parquet")



df_main.createOrReplaceTempView("crime_data")
query = """
WITH RankedCrimes AS (
    SELECT
        YEAR(`DATE OCC`) AS Year,
        MONTH(`DATE OCC`) AS Month,
        COUNT(*) AS Crime_Total,
        RANK() OVER (PARTITION BY YEAR(`DATE OCC`) ORDER BY COUNT(*) DESC) as Rank
    FROM crime_data
    GROUP BY YEAR(`DATE OCC`), MONTH(`DATE OCC`)
)
SELECT Year, Month, Crime_Total, Rank
FROM RankedCrimes
WHERE Rank <= 3
ORDER BY Year, Rank
"""
start_time = time.time()
top_crime_months_sql = spark.sql(query)

top_crime_months_sql.show(top_crime_months_sql.count())

end_time = time.time()
print("Execution time: {} seconds".format(end_time - start_time))
spark.stop()

