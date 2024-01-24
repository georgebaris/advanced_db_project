from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, TimestampType, LongType, DoubleType , DateType
from pyspark.sql.functions import col, to_date , when
from pyspark.sql import functions as F


# Create a Spark session
spark = SparkSession.builder.appName("LA_income_2015").getOrCreate()
# Define the schema for the DataFrame
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

file_path1 = "hdfs:///user/user/LA_income_2015.csv"
incdf15 = spark.read.csv(file_path1, header=True, inferSchema=False)

incdf15.printSchema()

print("Total number of rows:", incdf15.count())
incdf15.show()
# Save the DataFrame to a Parquet file
incdf15.write.parquet("hdfs:///user/user/income_2015.parquet")
spark.stop()


