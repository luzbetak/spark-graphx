#!/bin/bash
#----------------------------------------------------------#
#rm -fR temp/
sbt clean package

#----------------------------------------------------------#
spark-submit             \
  --class "SparkGraph"   \
  --master local[4]      \
  --driver-memory 4G     \
  --executor-memory 4G   \
  target/scala-2.11/spark-graph_2.12-1.0.jar      \
  /Users/kevin.duraj/github/spark-graph/words.txt \
  /Users/kevin.duraj/github/spark-graph/temp

#----------------------------------------------------------#
