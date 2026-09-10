#!/bin/bash
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
SPARK_HOME=/mnt/c/Users/Administrator/Desktop/spark-3.3.4-bin-hadoop3/spark-3.3.4-bin-hadoop3

echo "=== Task 3: KMeansClustering ==="

$SPARK_HOME/bin/spark-submit \
  --class KMeansClustering \
  --master 'local[4]' \
  /mnt/c/Users/Administrator/Desktop/final_exp/jar/task3.jar \
  file:///mnt/c/Users/Administrator/Desktop/final_exp/output/task1/user_features.csv \
  file:///mnt/c/Users/Administrator/Desktop/final_exp/output/task3

echo "Exit: $?"
