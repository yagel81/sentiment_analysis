#############################################################
######### For each article extract top three words ##########
######### And add it in a each article ######################
#############################################################



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()

# Read JSON data
wanted_date = str(datetime.date.today())
df = spark.read.json("hdfs://cnt7-naya-cdh63:8020/tmp/staging/project/api_calls/" + wanted_date + ".json")

# Select relevant columns
df = df.select(
    col('id'), col('sectionId'), col('sectionName'),
    col('webPublicationDate'), col('fields.headline'),
    col('fields.bodyText'), col('fields.wordcount'),
    col('fields.shortUrl'), col('fields.productionOffice')
)

# Function to clean words
def clean_word(s):
    return ''.join([ch for ch in s if ch.isalpha()])

# Extract words from bodyText
bodyText = df.select('bodyText').rdd.flatMap(lambda line: line.bodyText.split())
words = bodyText.flatMap(lambda line: line.split()).map(clean_word).filter(len)

# Count occurrences of each word
word_counts = words.groupBy(lambda word: word.lower()).map(lambda x: (x[0], len(x[1])))

# Read stop words file
stop_words = sc.textFile(r"/tmp/staging/other_files/english stop words.txt").map(lambda word: word.lower())

# Subtract stop words and sort by count
filtered_word_counts = word_counts.subtractByKey(stop_words).sortBy(lambda x: x[1], ascending=False)

# Create a DataFrame from the result
columns = ['word', 'count']
deptDF = spark.createDataFrame(filtered_word_counts, schema=columns)

# Add a new column for the extraction date
deptDF = deptDF.withColumn("date", lit(wanted_date))

# Save the DataFrame as JSON
output_path = "/tmp/staging/project/top_words_by_date/" + wanted_date + ".json"
deptDF.write.mode('overwrite').json(output_path)
print("top_words_by_date:", wanted_date)

# Save the JSON file to HDFS
file_path = glob.glob(output_path + "/*.json")[0]
with open(file_path, 'rb') as f:
    fs.upload('hdfs://cnt7-naya-cdh63:8020/tmp/staging/project/top_words_by_date/' + wanted_date + '.json', f)
print("top_words_by_date in hdfs:", wanted_date)
