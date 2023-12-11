"""
This script reads streaming data from Kafka, performs data transformations, and stores the processed data into both Elasticsearch 
and HDFS for further analysis.

It establishes a connection between Apache Spark and Kafka, reads real-time tweets, analyzes sentiment, and saves the enriched data 
to HDFS in JSON format.

Requirements:
- PySpark
- TextBlob
- Elasticsearch
- pyarrow
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from textblob import TextBlob
from pyspark.sql.types import StringType, StructType, IntegerType, FloatType
from elasticsearch_client import elasticsearch_client as es
import datetime
import glob
import pyarrow as pa
import time

# Get the current date
wanted_date = str(datetime.date.today())

# Set up the environment for connecting Spark and Kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 pyspark-shell'

# Kafka connection details
bootstrapServers = "cnt7-naya-cdh63:9092"
topics = "TweeterData"

# Create SparkSession
spark = SparkSession.builder.appName("ReadTweets").getOrCreate()

# Read streaming data from Kafka
df_kafka = spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrapServers).option("subscribe", topics).load()

# Define the schema for creating a DataFrame from JSON
schema = StructType() \
    .add("tweet_created_at", StringType()) \
    .add("location", StringType()) \
    .add("url", StringType()) \
    .add("tweet_id", StringType()) \
    .add("text", StringType()) \
    .add("user_acount_created_at", StringType()) \
    .add("user_id", StringType()) \
    .add("name", StringType()) \
    .add("followers_count", IntegerType()) \
    .add("friends_count", IntegerType()) \
    .add("listed_count", IntegerType())

# Convert JSON to DataFrame with the specified schema
df_tweets = df_kafka.select(col("value").cast("string")) \
    .select(from_json(col("value"), schema).alias("value")) \
    .select("value.*")

# Add current time in timestamp in column "current_ts"
df_tweets = df_tweets.withColumn("current_ts", current_timestamp())

# Add hour of current time in column "hour"
df_tweets = df_tweets.withColumn("hour", hour("current_ts").cast('integer'))

# Add minute of current time in column "minute"
df_tweets = df_tweets.withColumn("minute", minute("current_ts").cast('integer'))

# Add word count in column "wordCount"
df_tweets = df_tweets.withColumn('wordCount', size(split(col('text'), ' ')))

# Add sentiment analysis in column "wordCount"
def get_sentiment(string1):
    return TextBlob(string1).sentiment.polarity

get_sentiment_udf = udf(get_sentiment, FloatType())
df_tweets = df_tweets.withColumn('sentiment', get_sentiment_udf(col('text')))

# Initialize Elasticsearch client
elasticsearch_handler = es()

# Define function to store DataFrame batch into HDFS
def store_in_hdfs_batch(df, epoch_id):
    counter = 0
    print("---------------------Epoch Id is {} -----------------------------".format(epoch_id))
    while True:
        try:
            # Append to HDFS in JSON format
            df.write.mode('append').json("/tmp/staging/project/tweets/" + wanted_date + ".json")
            print("tweets: " + wanted_date)
            counter += 1

            # Save to HDFS every 1000 records
            if counter % 1000 == 0:
                # Save files in HDFS
                fs = pa.hdfs.connect(
                    host='cnt7-naya-cdh63',
                    port=8020,
                    user='hdfs',
                    kerb_ticket=None,
                    extra_conf=None)

                # Save tweets in HDFS
                file_path = glob.glob('/tmp/staging/project/tweets/' + wanted_date + '.json/*.json')[0]
                with open(file_path, 'rb') as f:
                    fs.upload(
                        'hdfs://cnt7-naya-cdh63:8020/tmp/staging/project/tweets/' + wanted_date + '.json', f)
                print("tweets in HDFS: " + wanted_date)

        except Exception as e:
            print(e)

# Write streaming DataFrame to HDFS
df_tweets \
    .writeStream \
    .foreachBatch(store_in_hdfs_batch) \
    .start() \
    .awaitTermination()
