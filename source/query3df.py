from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, count, rank, regexp_replace, regexp_extract, udf
from pyspark.sql.window import Window
from pyspark.sql.types import StringType , DoubleType
import time

spark = SparkSession.builder.appName("Q3df").getOrCreate()


# Read the DataFrame from the Parquet file
df_main = spark.read.parquet("hdfs:///user/user/crime_data.parquet")
geodf = spark.read.parquet("hdfs:///user/user/revgecoding.parquet")
incdf15 = spark.read.parquet("hdfs:///user/user/income_2015.parquet")

df_main.createOrReplaceTempView("df_main")
geodf.createOrReplaceTempView("geodf")
incdf15.createOrReplaceTempView("incdf15")



## Name matching with descent code chars 
descent_mapping = {
    'A': 'Asian',
    'B': 'Black',
    'C': 'Caucasian',
    'D': 'Indian',
    'F': 'Filipino',
    'G': 'German',
    'H': 'Hispanic',
    'I': 'Italian',
    'J': 'Japanese',
    'K': 'Korean',
    'L': 'Laotian',
    'O': 'Other',
    'P': 'Pacific Islander',
    'S': 'Samoan',
    'U': 'Hawaiian',
    'V': 'Vietnamese',
    'W': 'White',
    'X': 'Unknown',
    'Z': 'Asian Indian',
    '-': 'Not Specified',
    None: 'Unknown'  
}


def map_descent(code):
    return descent_mapping.get(code, 'Unknown')

map_descent_udf = udf(map_descent, StringType())



start_time = time.time()
## query df transformations 
crimes_2015_df = df_main.filter((year("DATE OCC") == 2015) & df_main["Vict Descent"].isNotNull())


crimes_2015_df_selected = crimes_2015_df.select("DR_NO", "DATE OCC", "AREA NAME", "Vict Descent", "LOCATION", "LAT", "LON")
geodf = geodf.withColumnRenamed("LAT", "LAT_df2").withColumnRenamed("LON", "LON_df2")


crime_with_zip_df = crimes_2015_df_selected.join(geodf, (crimes_2015_df.LAT == geodf.LAT_df2) & (crimes_2015_df.LON == geodf.LON_df2), "inner")
sel_cols = ["DR_NO", "DATE OCC", "AREA NAME", "Vict Descent", "LOCATION", "LAT", "LON", "ZIPcode"]
crime_with_zip_df = crime_with_zip_df.select(sel_cols)

incdf15 = incdf15.withColumn('Zip Code', col('Zip Code').cast(StringType()))

incdf15 = incdf15.withColumn("IncomeNumeric",
                            regexp_extract(col("Estimated Median Income"), r'[\d\.]+', 0).cast(DoubleType()))



crime_with_zip_df_income = crime_with_zip_df.join(incdf15, (crime_with_zip_df["ZIPcode"] == incdf15["Zip Code"]), "inner")



# crime_with_zip_df_income.show()
income_zip = crime_with_zip_df_income.select("Zip Code", "IncomeNumeric")

dist_zip_inc = income_zip.dropDuplicates(["Zip Code"])


top_3_zip_codes = dist_zip_inc.orderBy(col("IncomeNumeric").desc()).select("Zip Code").limit(3)


bot_3_zip_codes = dist_zip_inc.orderBy(col("IncomeNumeric").asc()).select("Zip Code").limit(3)


# keep top and bot zip codes to filter crime logs 

highlow_zip_list = top_3_zip_codes.select("Zip Code").rdd.flatMap(lambda x: x).collect() + \
 bot_3_zip_codes.select("Zip Code").rdd.flatMap(lambda x: x).collect()


highlow_zip_crimes = crime_with_zip_df.filter(col("ZIPcode").isin(highlow_zip_list))



crime_with_descent_names = highlow_zip_crimes.withColumn(
    'Vict Descent',
    map_descent_udf(col('Vict Descent'))
)

crime_counts_by_descent = crime_with_descent_names.groupBy('Vict Descent').agg(count('*').alias('Crime Count')).orderBy(col('Crime Count').desc())
 


crime_counts_by_descent.show(crime_counts_by_descent.count())


end_time = time.time()
print("Execution time: {} seconds".format(end_time - start_time))


spark.stop()


