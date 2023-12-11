from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, avg
from pyspark.sql.types import StringType, FloatType
from textblob import TextBlob
import datetime
import pyarrow as pa

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

# Set up HDFS connection
fs = pa.hdfs.connect(
    host='cnt7-naya-cdh63',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    extra_conf=None
)

# Define the current date
wanted_date = str(datetime.date.today())

# Read top words data
df_top_words = spark.read.json(f"/tmp/staging/project/top_words_by_date/{wanted_date}.json").select('word').distinct()

# Create a daily word string
daily_word = df_top_words.limit(3).agg(concat_ws(' ', collect_list('word')).alias('daily_word')).collect()[0]['daily_word']

# Read articles data from HDFS
df = spark.read.json(f"hdfs://cnt7-naya-cdh63:8020/tmp/staging/project/api_calls/{wanted_date}.json")

# Select relevant columns
df = df.select(
    col('id').alias('news_id'), col('sectionId').alias('section_id'),
    col('sectionName').alias('section_name'), col('webPublicationDate').alias('web_publication_date'),
    col('fields.headline').alias('headline'), col('fields.bodyText').alias('body_text'),
    col('fields.wordcount'), col('fields.shortUrl').alias('short_url'),
    col('fields.productionOffice').alias('production_office')
)

# Filter articles that have more than 10 words
df = df.filter(col('wordcount') > 10)

# Add a lower column of body text
lower_words_UDF = udf(lambda z: z.lower(), StringType())
df = df.withColumn('body_text_lower_words', lower_words_UDF(col('body_text')))
df = df.filter(df.body_text_lower_words.contains(daily_word))

# Add sentiment analysis in a new column
get_sentiment_udf = udf(lambda string1: TextBlob(string1).sentiment.polarity, FloatType())
df = df.withColumn('sentiment', get_sentiment_udf(col('body_text')))

# Add a column for the daily top word
df = df.withColumn('daily_top_word', lit(daily_word))

# Save the DataFrame as JSON
articles_output_path = f"/tmp/staging/project/articles_with_sentiment_filtered_by_daily_word/{wanted_date}.json"
df.write.mode('overwrite').json(articles_output_path)
print(f"articles_with_sentiment_filtered_by_daily_word: {wanted_date}")

# Calculate the average sentiment
average_sentiment = df.select(avg('sentiment')).collect()[0][0]

# Read the top words DataFrame
word_df = spark.read.json(f"/tmp/staging/project/top_words_by_date/{wanted_date}.json")

# Select relevant columns
word_df = word_df.select(col('word'), col('count'), col('date'))

# Add a column for the average sentiment
word_df = word_df.withColumn('average_sentiment_column', lit(average_sentiment))

# Save the DataFrame as JSON
top_words_output_path = f"/tmp/staging/project/top_words_by_date_with_avg_sentiment/{wanted_date}.json"
word_df.write.mode('overwrite').json(top_words_output_path)
print(f"top_words_by_date_with_avg_sentiment: {wanted_date}")

# Save files in HDFS
for file_path in [articles_output_path, top_words_output_path]:
    hdfs_path = f"hdfs://cnt7-naya-cdh63:8020{file_path}"
    with open(file_path, 'rb') as f:
        fs.upload(hdfs_path, f)
