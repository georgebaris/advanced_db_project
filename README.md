# Advanced Topics in Database Systems Semester Project

This repository is for the semester project of the course "Advanced Topics in Database Systems" of 9<sup>th</sup>
 semester.


---
**Contributors**:
- Georgios Baris - / [georgebaris](https://github.com/georgebaris)
- Ioannis Konstantinos Chatzis - / [jcchatzis](https://github.com/jcchatzis)
---

**School of Electrical and Computer Engineering**  
**National Technical University of Athens**

_January 2024_

---
## Repo Description
> Source code can be found in the ["source"](https://github.com/georgebaris/advanced_db_project/tree/main/source) folder. Contains Dataframe Creation and Query Execution files.

1. Installation of Spark on Virtual Machines or locally by following the steps in the [Spark Setup Guide](https://colab.research.google.com/drive/1pjf3Q6T-Ak2gXzbgoPpvMdfOHd1GqHZG?usp=sharing)

2. Create a folder where all the `.csv` files will be placed. 
3. The files required for the execution of the exercise can be found at the following links:
   - [Crime Data from 2010 to 2019](https://catalog.data.gov/dataset/crime-data-from-2010-to-2019)
   - [Crime Data from 2020 to Present](https://catalog.data.gov/dataset/crime-data-from-2020-to-present)
   - [Income Data 2015](http://www.laalmanac.com/employment/em12c_2015.php)
   - [DBLab NTUA Data Files](http://www.dblab.ece.ntua.gr/files/classes/data.tar.gz)

4. Add these files to the dfs with the command: `hdfs dfs -put "FILENAME".csv hdfs:///user/user/`
5. Before executing the queries, the following commands must be run to create the necessary .parquet files:

```shell
spark-submit create_dataframe.py
spark-submit lapd_dataframe.py
spark-submit geo_dataframe.py
spark-submit income_dataframe.py
```
Or
```shell
chmod +x dataframes_run.sh
./dataframes_run.sh
```

6. The queries are executed with the following commands:
```shell
spark-submit --num-executors N queryXAPI.py
```

> where **N** is the number of executors, **X** is the query number and **API** is the API used (DF, SQL or RDD)

> Query example: execution and output.
 
`spark-submit --num-executors 4 query4df_1a.py`

```shell
Execution time: 12.7367 seconds

+------+----------------------+-------+
| Year | Average Distance (km)| Count |
+------+----------------------+-------+
| 2010 | 2.651                | 5486  |
| 2011 | 2.793                | 7232  |
| 2012 | 2.836                | 6532  |
| 2013 | 2.731                | 2271  |
| 2014 | 2.683                | 2320  |
| 2015 | 2.654                | 3500  |
| 2016 | 2.734                | 6076  |
| 2017 | 2.724                | 7786  |
| 2018 | 2.566                | 2242  |
| 2019 | 2.74                 | 7129  |
| 2020 | 2.49                 | 2248  |
| 2021 | 2.64                 | 9745  |
| 2022 | 2.609                | 10026 |
| 2023 | 2.556                | 9017  |
+------+----------------------+-------+
```
or run the shell script:
```shell
chmod +x queries_run.sh
./queries_run.sh N X

```
where **N** is the number of query chosen (1,2,3 or 4) and **X** is the case of query 4 and the chosen subquery (1a,1b,2a,2b).

> **Note**: If you are having _trouble_ with the _execution_ of queries, having the following _error raised_: `Error: moodule udfs not found`, know the following:
> the queries are implemented using _[User Defined Functions](https://sparkbyexamples.com/pyspark/pyspark-udf-user-defined-function/)_ which have to be _registered_ to the hdfs before execution. So you have to either **register** the module to your system, or **uncommnet** the segments of the code mentioned in each query file and run the queries again.

7. The results of the queries are printed as DataFrame views, and the execution time is printed in the terminal.
8. For further investigation of efficiency of joins, use `hint("joinType")` or `explain()` to print the join strategy.
---
## Project Report Description
The report can be found in the ["report"]() folder. Contains the report in pdf format and the latex files. 

In the report you can find:
- The motivation and the objective of this project 
- The Spark framework description
- The initial data manipulation,
- Query description and performance for each API
- Statistics for the different types of joins
- Appendix with the code of chosen queries
---