
"""
This script reads data from a JSON file containing articles with sentiment information, stored in a PySpark DataFrame.
It then converts the PySpark DataFrame to a Pandas DataFrame and iterates over each row, creating records and storing
them in Elasticsearch using the elasticsearch_client.

Requirements:
- PySpark
- Pandas
- Elasticsearch
"""
from pyspark.sql import SparkSession
import pandas as pd
import datetime
from elasticsearch_client import elasticsearch_client

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

def test_create_new_record(record):
    elasticsearch_client().store_record(id=record["news_id"], record=record)

# Define the current date
wanted_date = str(datetime.date.today())

# Read data from JSON file into a PySpark DataFrame
local_path = f'/tmp/staging/project/articles_with_sentiment_filtered_by_daily_word/{wanted_date}.json'
df = spark.read.json(local_path)

# Convert PySpark DataFrame to Pandas DataFrame
df_pandas = df.toPandas()

# Iterate over each row in the Pandas DataFrame and call test_create_new_record
for _, row in df_pandas.iterrows():
    record = {
        "news_id": row["news_id"],
        "section_id": row["section_id"],
        "section_name": row["section_name"],
        "web_publication_date": row["web_publication_date"],
        "headline": row["headline"],
        "body_text": row["body_text"],
        "wordcount": row["wordcount"],
        "short_url": row["short_url"],
        "production_office": row["production_office"],
        "body_text_lower_words": row["body_text_lower_words"],
        "sentiment": row["sentiment"],
        "daily_top_word": row["daily_top_word"]
    }
    test_create_new_record(record)
"""
This script reads article data with sentiment information for a specific date from a JSON file using PySpark.
It converts the PySpark DataFrame to a Pandas DataFrame and iterates over each row, creating records.
These records are then stored in Elasticsearch using the elasticsearch_client.
"""
