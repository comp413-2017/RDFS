#!/usr/bin/env bash

hdfs dfs -fs hdfs://localhost:5351 -mkdir /dir1
hdfs dfs -fs hdfs://localhost:5351 -mkdir /dir2
hdfs dfs -fs hdfs://localhost:5351 -mkdir /dir3

hdfs dfs -fs hdfs://localhost:5351 -rm -skipTrash /dir2
hdfs dfs -fs hdfs://localhost:5351 -rmr -skipTrash /dir2

hdfs dfs -fs hdfs://localhost:5351 -mkdir /dir1/dir1_1
hdfs dfs -fs hdfs://localhost:5351 -mkdir /dir1/dir1_2

hdfs dfs -fs hdfs://localhost:5351 -touchz /dir1
hdfs dfs -fs hdfs://localhost:5351 -touchz /file1

hdfs dfs -fs hdfs://localhost:5351 -touchz /dir1/dir1_file1