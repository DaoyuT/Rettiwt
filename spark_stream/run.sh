#!/bin/bash

SPARK_ADDR=spark://ec2-52-6-127-71.compute-1.amazonaws.com:7077

sbt assembly
sbt package

if [ $# -ne 1 ]; then
  echo "Running default Cassandra_Norm..."
  spark-submit --class TweetDataStreaming --master $SPARK_ADDR --jars target/scala-2.10/tweet_data-assembly-1.0.jar target/scala-2.10/tweet_data_2.10-1.0.jar
elif [ $1 -eq 1 ]; then
  echo "Running Cassandra_Norm..."
  spark-submit --class TweetDataStreaming_Norm --master $SPARK_ADDR --jars target/scala-2.10/tweet_data-assembly-1.0.jar target/scala-2.10/tweet_data_2.10-1.0.jar
elif [ $1 -eq 2 ]; then
  echo "Running Cassandra_Denorm..."
  spark-submit --class TweetDataStreaming_Denorm --master $SPARK_ADDR --jars target/scala-2.10/tweet_data-assembly-1.0.jar target/scala-2.10/tweet_data_2.10-1.0.jar
elif [ $1 -eq 3 ]; then
  echo "Running Redis_Norm..."
  spark-submit --class TweetDataStreaming_Redis_Norm --master $SPARK_ADDR --jars target/scala-2.10/tweet_data-assembly-1.0.jar target/scala-2.10/tweet_data_2.10-1.0.jar
elif [ $1 -eq 4 ]; then
  echo "Running Redis_Denorm..."
  spark-submit --class TweetDataStreaming_Redis_Denorm --master $SPARK_ADDR --jars target/scala-2.10/tweet_data-assembly-1.0.jar target/scala-2.10/tweet_data_2.10-1.0.jar
else
  echo "Running default Cassandra_Norm..."
  spark-submit --class TweetDataStreaming --master $SPARK_ADDR --jars target/scala-2.10/tweet_data-assembly-1.0.jar target/scala-2.10/tweet_data_2.10-1.0.jar
fi