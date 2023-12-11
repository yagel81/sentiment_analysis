"""
This script retrieves articles with sentiment information for a specific date, processes the data, and sends a daily
news update email to the user. Additionally, it stores relevant information in a local MySQL database.

Requirements:
- SparkSession
- Pandas
- Requests
- Elasticsearch
- MySQL Connector
- smtplib (for email notifications)
"""

import requests
import json
import os
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
import pandas as pd
import datetime
import re
import smtplib
import mysql.connector

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

# Collect articles from a specific date and convert them to a dictionary
wanted_date = str(datetime.date.today())
local_path = f'/tmp/staging/project/articles_with_sentiment_filtered_by_daily_word/{wanted_date}.json'
df = spark.read.json(local_path).toPandas().to_dict('records')

# Extract variables for use in notifications
df = pd.DataFrame(df)

# Sort dataframe for negative sentiment
df_bad = df.sort_values('sentiment')
df_most_common_word = df_bad['daily_top_word'].iloc[0]
df_bad_url = df_bad['short_url'].iloc[0]
df_bad_sentiment = str(df_bad['sentiment'].iloc[0])
df_bad_headline = df_bad['headline'].iloc[0]
df_bad_headline = ' '.join([i for i in re.split(r'[^A-Za-z]', df_bad_headline) if i])  # Remove non-alphabetic words

# Sort dataframe for positive sentiment
df_positive = df.sort_values(by='sentiment', ascending=False)
df_positive_url = df_positive['short_url'].iloc[0]
df_positive_sentiment = str(df_positive['sentiment'].iloc[0])
df_positive_headline = df_positive['headline'].iloc[0]
df_positive_headline = ' '.join([i for i in re.split(r'[^A-Za-z]', df_positive_headline) if i])  # Remove non-alphabetic words

df_articles_count = str(len(df))

### Send email to the user with notifications

gmail_user = '<gmail_user>'
gmail_password = '<password>'

sent_from = gmail_user
to = '<recipient email>'
subject = 'Here are your daily news updates - ' + wanted_date

body = f'''Good morning,
As of {wanted_date}, the most common word is "{df_most_common_word}" with {df_articles_count} articles.
The article with the most negative sentiment, with a score of {df_bad_sentiment}, is: {df_bad_headline}
{df_bad_url}
The article with the positive sentiment, with a score of {df_positive_sentiment}, is: {df_positive_headline}
{df_positive_url}
'''

email_text = f"""\
From: {sent_from}
To: {to}
Subject: {subject}

{body}
"""

try:
    smtp_server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    smtp_server.ehlo()
    smtp_server.login(gmail_user, gmail_password)
    smtp_server.sendmail(sent_from, to, email_text)
    smtp_server.close()
    print("Email sent successfully!")
except Exception as ex:
    print("Something went wrong….", ex)

## save data in local mysql

mydb = mysql.connector.connect(
    host='<host>',
    user='<user_name>',
    password='<password>',
    database='final_project'
)

cursor = mydb.cursor()
cursor.execute(
    """INSERT INTO daily_news VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
    (df_most_common_word, df_bad_url, df_bad_sentiment, df_bad_headline,
     df_positive_url, df_positive_sentiment, df_positive_headline, wanted_date)
)
mydb.commit()
print(cursor.rowcount, "Record inserted successfully into api_results table")
cursor.close()

"""
This script fetches articles with sentiment information for a specific date, processes the data, and sends a daily
news update email to the user. Additionally, it stores relevant information in a local MySQL database.
"""
