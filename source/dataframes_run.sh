#!/bin/bash


SPARK_SUBMIT=/home/user/opt/spark/bin/spark-submit
#change this to your spark-submit path


echo "Running create_dataframe.py"
$SPARK_SUBMIT create_dataframe.py

echo "Running geo_dataframe.py"
$SPARK_SUBMIT geo_dataframe.py

echo "Running income_dataframe.py"
$SPARK_SUBMIT income_dataframe.py

echo "Running lapd_dataframe.py"
$SPARK_SUBMIT lapd_dataframe.py

echo "All dataframe creation jobs completed."
