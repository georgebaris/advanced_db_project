#!/bin/bash

SPARK_SUBMIT=/home/user/opt/spark/bin/spark-submit
#change this to your spark-submit path

# Function to run query
run_query() {
    local query_file="$1"
    if [[ -f "$query_file" ]]; then
        echo "Running $query_file"
        $SPARK_SUBMIT "$query_file"
    else
        echo "File not found: $query_file"
    fi
}


if [[ $1 -lt 1 || $1 -gt 4 ]]; then
    echo "Provide a number between 1 to 4 for queries."
    exit 1
fi

# case for the 4th query and run the query for the given argument
if [[ $1 -eq 4 ]]; then
    if [[ $2 =~ ^(1a|1b|2a|2b)$ ]]; then
        run_query "query4df_$2.py"
        run_query "query4sql_$2.py"
    else
        echo "Query 4: second argument must be one of 1a, 1b, 2a, 2b."
        exit 1
    fi
else
    # Handle other cases and run all queries
    for file in query$1*.py; do
        run_query "$file"
    done
fi
