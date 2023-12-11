"""
This script initializes a Twitter stream listener using Tweepy, capturing tweets related to a specific daily word. 
The collected tweet data is then processed, filtered, and sent to Kafka topics 'TweeterArchive2' and 'TweeterData'.

Requirements:
- Tweepy
- Kafka
- PySpark
"""

import json
import tweepy
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, StringType, explode
import datetime

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Twitter API credentials
consumer_key = '<consumer_key>'
consumer_secret = '<consumer_secret >'
access_token = '<access_token>'
access_secret = '<access_secret>'

# Identify the most common word for the day
wanted_date = str(datetime.date.today())
df = spark.read.json("/tmp/staging/project/top_words_by_date/" + wanted_date + ".json").toPandas().to_dict('records')
daily_word = " " + str(df[0]['word']) + " "

# Twitter Stream Listener Class
class TwitterStreamListener(tweepy.StreamListener):
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers='cnt7-naya-cdh63',
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.tweets = []

    def on_data(self, data):
        # Data is the full tweet JSON data
        api_events = json.loads(data)

        # Gathering relevant values
        # Event-related values
        event_keys = ['created_at', 'id', 'text']
        twitter_events = {k: v for k, v in api_events.items() if k in event_keys}
        twitter_events['tweet_created_at'] = twitter_events.pop('created_at')
        twitter_events['tweet_id'] = twitter_events.pop('id')
        # User-related values
        user_keys = ['id', 'name', 'created_at', 'location', 'url', 'protected', 'verified',
                     'followers_count', 'friends_count', 'listed_count', 'favourites_count',
                     'statuses_count', 'withheld_in_countries']
        user_events = {k: v for k, v in api_events['user'].items() if k in user_keys}
        user_events['user_acount_created_at'] = user_events.pop('created_at')
        user_events['user_id'] = user_events.pop('id')

        # Merge dictionaries
        user_events.update(twitter_events)
        events = user_events

        # Send data to Kafka topic(s)
        self.producer.send('TweeterArchive2', events)
        self.producer.send('TweeterData', events)
        self.producer.flush()

        print(events)

    def on_error(self, status_code):
        if status_code == 420:
            return False


def initialize():
    # Authenticate with Twitter API
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)

    # Initialize Twitter stream listener
    stream = TwitterStreamListener()
    twitter_stream = tweepy.Stream(auth=api.auth, listener=stream)
    
    # Filter tweets based on the daily word and English language
    twitter_stream.filter(track=[daily_word], languages=['en'])


# Initialize the Twitter stream listener
initialize()
