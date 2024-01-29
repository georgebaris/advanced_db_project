from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, count, rank, regexp_replace, regexp_extract, udf
from pyspark.sql.types import StringType
import time


spark = SparkSession.builder.appName("Q3sql").getOrCreate()


# Read the DataFrame from the Parquet file
df_main = spark.read.parquet("hdfs:///user/user/crime_data.parquet")
geodf = spark.read.parquet("hdfs:///user/user/revgecoding.parquet")
incdf15 = spark.read.parquet("hdfs:///user/user/income_2015.parquet")

df_main.createOrReplaceTempView("df_main")
geodf.createOrReplaceTempView("geodf")
incdf15.createOrReplaceTempView("incdf15")


start_time = time.time()


spark.sql("""
    -- Filter for 2015 crimes and select required columns
    WITH Crimes2015 AS (
        SELECT DR_NO, `DATE OCC`, `AREA NAME`, `Vict Descent`, LOCATION, LAT, LON
        FROM df_main
        WHERE YEAR(`DATE OCC`) = 2015 AND `Vict Descent` IS NOT NULL
    ),
    -- Join with geodf to get ZIP codes
    CrimeWithZip AS (
        SELECT c.*, g.ZIPcode
        FROM Crimes2015 c
        INNER JOIN geodf g ON c.LAT = g.LAT AND c.LON = g.LON
    ),
    -- Join with income data
    CrimeWithIncome AS (
    SELECT cwz.*, i.`Zip Code`, 
           CAST(REGEXP_REPLACE(i.`Estimated Median Income`, '[\\$,]', '') AS DOUBLE) AS IncomeNumeric
    FROM CrimeWithZip cwz
    INNER JOIN incdf15 i ON cwz.ZIPcode = i.`Zip Code`
    ),
    -- Get distinct ZIP codes based on income
    DistinctZipIncome AS (
        SELECT DISTINCT `Zip Code`, IncomeNumeric
        FROM CrimeWithIncome
    ),
    -- Get top 3 and bottom 3 ZIP codes
    TopBottomZip AS (
        (SELECT `Zip Code` FROM DistinctZipIncome ORDER BY IncomeNumeric DESC LIMIT 3)
        UNION ALL
        (SELECT `Zip Code` FROM DistinctZipIncome ORDER BY IncomeNumeric ASC LIMIT 3)
    ),
    -- Filter crimes for these ZIP codes
    FilteredCrimes AS (
        SELECT *
        FROM CrimeWithIncome
        WHERE `Zip Code` IN (SELECT `Zip Code` FROM TopBottomZip)
    )
    SELECT 
        COALESCE(
    CASE `Vict Descent`
        WHEN 'A' THEN 'Asian'
        WHEN 'B' THEN 'Black'
        WHEN 'C' THEN 'Caucasian'
        WHEN 'D' THEN 'Indian'
        WHEN 'F' THEN 'Filipino'
        WHEN 'G' THEN 'German'
        WHEN 'H' THEN 'Hispanic'
        WHEN 'I' THEN 'Italian'
        WHEN 'J' THEN 'Japanese'
        WHEN 'K' THEN 'Korean'
        WHEN 'L' THEN 'Laotian'
        WHEN 'O' THEN 'Other'
        WHEN 'P' THEN 'Pacific Islander'
        WHEN 'S' THEN 'Samoan'
        WHEN 'U' THEN 'Hawaiian'
        WHEN 'V' THEN 'Vietnamese'
        WHEN 'W' THEN 'White'
        WHEN 'X' THEN 'Unknown'
        WHEN 'Z' THEN 'Asian Indian'
        WHEN '-' THEN 'Not Specified'
        ELSE 'Unknown'
    END,
    'Unknown'
) AS `Vict Descent Name`,
        COUNT(*) AS `Crime Count`
    FROM FilteredCrimes
    GROUP BY `Vict Descent Name`
    ORDER BY `Crime Count` DESC
""").show()

end_time = time.time()
print("Execution time: {} seconds".format(end_time - start_time))

spark.stop()



