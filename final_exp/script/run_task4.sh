#!/bin/bash
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
SPARK_HOME=/mnt/c/Users/Administrator/Desktop/spark-3.3.4-bin-hadoop3/spark-3.3.4-bin-hadoop3

echo "=== Task 4: PersonalRank ==="

$SPARK_HOME/bin/spark-submit \
  --class PersonalRank \
  --master 'local[4]' \
  /mnt/c/Users/Administrator/Desktop/final_exp/jar/task4.jar \
  file:///mnt/c/Users/Administrator/Desktop/final_exp/data/train.csv \
  file:///mnt/c/Users/Administrator/Desktop/final_exp/data/test.csv \
  100004 \
  file:///mnt/c/Users/Administrator/Desktop/final_exp/output/task4

echo "Exit: $?"
