from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, count, rank
from pyspark.sql.window import Window
import time

# Start timing the query
spark = SparkSession.builder.appName("Q1df").getOrCreate()


# Read the DataFrame from the Parquet file
df_main = spark.read.parquet("hdfs:///user/user/crime_data.parquet")


# create view
df_main.createOrReplaceTempView("df_main")

start_time = time.time()

# Add Year and Month columns based on 'DATE OCC'
crime_df = df_main.withColumn('Year', year('DATE OCC')).withColumn('Month', month('DATE OCC'))

# Count crimes per year and month
crime_counts = crime_df.groupBy('Year', 'Month').agg(count('*').alias('Crime_Total'))

# Set up ranking within each year by crime count
windowSpec = Window.partitionBy('Year').orderBy(col('Crime_Total').desc())
crime_counts = crime_counts.withColumn('Rank', rank().over(windowSpec))

# filter top 3 months per year
top_crime_months = crime_counts.filter(col('Rank') <= 3).orderBy('Year', col('Crime_Total').desc())



top_crime_months.show(top_crime_months.count())
end_time = time.time()
print("Execution time: {} seconds".format(end_time - start_time))
spark.stop()
