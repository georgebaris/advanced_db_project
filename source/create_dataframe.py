
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, TimestampType, LongType, DoubleType , DateType
from pyspark.sql.functions import col, to_date , when


# Create a Spark session
spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()
# Define the schema for the DataFrame
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


crime_schema = StructType([
        StructField("DR_NO", StringType(), nullable=True),
        StructField("Date Rptd", StringType(), True),
        StructField("DATE OCC", StringType(),nullable=True),    
        StructField("TIME OCC", StringType(), nullable=True),
        StructField("AREA", StringType(), nullable=True),
        StructField("AREA NAME", StringType(), nullable=True),
        StructField("Rpt Dist No", StringType(), nullable=True),
        StructField("Part 1-2", LongType(), nullable=True),
        StructField("Crm Cd", LongType(), nullable=True),
        StructField("Crm Cd Desc", StringType(), nullable=True),
        StructField("Mocodes", StringType(), nullable=True),
        StructField("Vict Age", IntegerType(), nullable=True),
        StructField("Vict Sex", StringType(), nullable=True),
        StructField("Vict Descent", StringType(), nullable=True),
        StructField("Premis Cd", LongType(), nullable=True),
        StructField("Premis Desc", StringType(), nullable=True),
        StructField("Weapon Used Cd", LongType(), nullable=True),
        StructField("Weapon Desc", StringType(), nullable=True),
        StructField("Status", StringType(), nullable=True),
        StructField("Status Desc", StringType(), nullable=True),
        StructField("Crm Cd 1", LongType(), nullable=True),
        StructField("Crm Cd 2", LongType(), nullable=True),
        StructField("Crm Cd 3", LongType(), nullable=True),
        StructField("Crm Cd 4", LongType(), nullable=True),
        StructField("LOCATION", StringType(), nullable=True),
        StructField("Cross Street", StringType(), nullable=True),
        StructField("LAT", DoubleType(), nullable=True),
        StructField("LON",DoubleType(), nullable=True)
        ])

# Assuming your dataset is in a CSV file named "crime_data.csv"

file_path1 = "hdfs:///user/user/Crime_Data_from_2010_to_2019.csv"
file_path2 = "hdfs:///user/user/Crime_Data_from_2020_to_present.csv"

df1 = spark.read.csv(file_path1, header=True, inferSchema=False, schema=crime_schema)
df2 = spark.read.csv(file_path2, header=True, inferSchema=False, schema=crime_schema)


df_main = df1.union(df2)

df_main = df_main.withColumn("Date Rptd", to_date(col("Date Rptd"), "MM/dd/yyyy")) \
             .withColumn("DATE OCC", to_date(col("DATE OCC"), "MM/dd/yyyy"))
df_main = df_main.withColumn("Date Rptd", when(col("Date Rptd").isNotNull(), to_date(col("Date Rptd"), "MM/dd/yyyy"))) \
                 .withColumn("DATE OCC", when(col("DATE OCC").isNotNull(), to_date(col("DATE OCC"), "MM/dd/yyyy")))

df_main = df_main.dropDuplicates(subset=["DR_NO"])
# Print the schema and total number of rows
df_main.printSchema()
print("Total number of rows:", df_main.count())
df_main.show()

# HDFS path to check
hdfs_path = "hdfs:///user/user/crime_data.parquet"

# Check if the file exists
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
file_exists = fs.exists(spark._jvm.org.apache.hadoop.fs.Path(hdfs_path))

# If the file doesn't exist, write the Parquet file
if not file_exists:
    # Assuming df_main is your DataFrame
    df_main.write.parquet(hdfs_path)
    print("Parquet file written successfully.")
else:
    print("Parquet file already exists. Skipping write operation.")


spark.stop()


