#!/bin/bash
for i in `seq 1 $2`;
do
    hdfs dfs -fs hdfs://localhost:5351 -copyFromLocal $1 /copy_test_$i
    echo "Copied the ${i}th version of $1"
done
