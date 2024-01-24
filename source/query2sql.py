from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, count, rank
from pyspark.sql.window import Window
import time

spark = SparkSession.builder.appName("Q2sql").getOrCreate()


# Read the DataFrame from the Parquet file
df_main = spark.read.parquet("hdfs:///user/user/crime_data.parquet")


df_main.createOrReplaceTempView("crime_data")


day_segment_query = """
SELECT
    CASE
        WHEN SUBSTR(`TIME OCC`, 1, 2) BETWEEN '05' AND '11' THEN 'Morning'
        WHEN SUBSTR(`TIME OCC`, 1, 2) BETWEEN '12' AND '16' THEN 'Afternoon'
        WHEN SUBSTR(`TIME OCC`, 1, 2) BETWEEN '17' AND '20' THEN 'Evening'
        ELSE 'Night'
    END AS Day_Segment,
    COUNT(*) AS Crime_Count
FROM crime_data
WHERE `Premis Desc` = 'STREET'
GROUP BY Day_Segment
ORDER BY Crime_Count DESC
"""
start_time = time.time() 
day_segment_crimes = spark.sql(day_segment_query)
day_segment_crimes.show()

end_time = time.time()
print("Execution time: {} seconds".format(end_time - start_time))

spark.stop()
