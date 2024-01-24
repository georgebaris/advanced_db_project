from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, TimestampType, LongType, DoubleType , DateType
from pyspark.sql.functions import col, to_date , when
from pyspark.sql import functions as F


# Create a Spark session
spark = SparkSession.builder.appName("revgecoding").getOrCreate()
        # Define the schema for the DataFrame
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

file_path1 = "hdfs:///user/user/revgecoding.csv"
geodf = spark.read.csv(file_path1, header=True, inferSchema=False)
geodf.printSchema()

print("Total number of rows:", geodf.count())
geodf.show()
# Save the DataFrame to a Parquet file
geodf.write.parquet("hdfs:///user/user/revgecoding.parquet")

spark.stop()

                                                   
