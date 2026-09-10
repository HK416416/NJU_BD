#!/bin/bash
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
SPARK_HOME=/mnt/c/Users/Administrator/Desktop/spark-3.3.4-bin-hadoop3/spark-3.3.4-bin-hadoop3

echo "=== Task 2: InvertedIndexCooccur ==="

$SPARK_HOME/bin/spark-submit \
  --class InvertedIndexCooccur \
  --master 'local[4]' \
  /mnt/c/Users/Administrator/Desktop/final_exp/jar/task2.jar \
  file:///mnt/c/Users/Administrator/Desktop/final_exp/data/train.csv \
  file:///mnt/c/Users/Administrator/Desktop/final_exp/data/test.csv \
  file:///mnt/c/Users/Administrator/Desktop/final_exp/output/task2

echo "Exit: $?"
