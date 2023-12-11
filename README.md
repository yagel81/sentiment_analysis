# Sentiment Analysis
 This project aims to test the correlation between news sentiment and public opinion by analyzing the sentiment scores of popular news articles and corresponding real-time tweets related to the most common topic, revealing insights into sentiment changes over time.

# Background
The year 2022 is a fast-paced world where dramatic events occur almost every day.
The number of events that take place every day is enormous and our ability, as human beings, to process these events and form an opinion about them becomes a very difficult task.
Usually, because events do not take place in our immediate vicinity, we tend to catch up on news from two main sources - news and social networks.
Both of these sources of information are critical in obtaining objective information.
Sometimes, and often the coverage of events, in the news and on social networks is done not objectively but in a biased way that affects the way we treat these events - in a positive or negative way.





# Purpose of the project 
The purpose of this project is to set an environment for testing the correlation between the sentiments of tweets and articles news.
Is it because news covers a particular event positively that public opinion also responds positively?

The study analyzes the sentiment score of the most popular news.
To know what is the “hottest” topic, we extracted from all the articles the common word and calculated its average sentiment score according to the articles written about him.
We then collected real-time tweets that contained the most common word and also analyzed her sentiment score on it.
The combination of these two sources of data allows us to see first and foremost whether there is a correlation in sentiment score or not, whether sentiment score changes over time, and who leads to a change in sentiment score.


# Data sources
In this project we connected imported data from two main sources.
That allowed us to analyze the correlation between them:
Guardian
The Guardian is a British newspaper published in 3 different countries - England, USA and Australia.
His articles are mostly printed in print newspapers but are also published in the digital newspaper.
Every day hundreds of different articles are posted on the Guardian website about all the events taking place in the world.
The Guardian is a global news site with a lot of media influence. This was one of the reasons why we chose this source of information as a news source.
API connection – the Guardian provide the ability to extract easily articles by an API for a selected time frame.
That is one of the reasons we choose that source 
Twitter 
Twitter has grown in popularity during the past decades. It is now used by millions of users who share information about their daily life and their opinions on certain topics in the media.
API connection – Twitter allows to have a connectivity by an API and collect all the twits that contain specific words.


Choosing the combination of these two sources give us the ability to automate the extraction of hot topics in world and find the public opinion on Twitter.
High Level Architecture Overview

Despite the complexity of the open-source tool, we decided to go for an architecture that consists of only open-source tools for a number of reasons - the main ones being:
1) Pricing: The sourcing tools are free and therefore the uncertainty regarding future payments disappears and is irrelevant.
Of course, it is important to note that operating in an open-source world requires other resources and attention to the management and supervision of the above tools, the above resources often require higher aggregate costs due to a high number of employees and experts who will develop and support the various systems. However, in environments that contain a high amount of data - overall cost savings can be seen over time.

2) Flexibility: Using these tools allows us full flexibility to switch between different technologies, and does not limit it to the different tools that the same cloud provider we decide to work with has. The number of organizations that will adopt more than one cloud provider is extremely low due to the complexity and variety of knowledge that the organization needs to acquire, so the hybrid look is not taken as a viable alternative.

* Another emphasis that is important for us to note is the focus on developed, stable and mature technologies that have provided years in the field and have a strong and supportive aura that can serve as a help for us if needed.

# Pipe-line
![alt text](https://github.com/yagel81/sentiment_analysis/blob/main/other/data%20pipeline%20project.jpg?raw=true)

# Comparisons between different systems
# Storage 
Chosen tech – HDFS
Distributed data storage service that supports large amounts of data using the following principles:
Cluster - Allows you to scale out and add servers as needed.
High availability - With the ability to replicate data more than once, we enjoy available information even if one of our servers in the cluster has fallen and the information on it is not accessible to us.
In addition, with the help of data duplication we can produce more efficient work distribution by dividing the servers in the cluster into write / read servers according to needs.
Free service - with natural integration and a large community in the worlds of big data.
Alternative tools - ADLS/ S3
The above services have been disqualified because they are SASS provided by cloud service providers, and we have preferred not to be limited to the whims of one cloud provider or another that in the future may increase the price of the service significantly.

# DB/DWH tool
# Chosen tech – Elastic search
A both operational and analytical database service that allows information to be stored without a pre-determined schema, based on a cluster.
This service basically contains the Lucin Index which indexes the words themselves and thus enables fast and advanced retrieval on text analysis with the help of simple questioning of simple esquire queries.
Elastic is very fast and by using distributed inverted indices, Elasticsearch quickly finds the best matches for your full-text searches from even very large data sets.
We can query the data by KQL or http requests easily 
Multilingual – The ICU plugin is used to index and tokenize multilingual content which is an Elasticsearch plugin based on the lucene implementation of the unicode text segmentation standard. Based on character ranges, it decides whether to break on a space or character.
Scalability - Elasticsearch is built to scale. It will run perfectly fine on any machine or in a cluster containing hundreds of nodes, and the experience is almost identical. Growing from a small cluster to a large cluster is almost entirely automatic and painless. Growing from a large cluster to a very large cluster requires a bit more planning and design, but it is still relatively painless
Alternative tools – Mongo DB / MySQL
We disqualified the rest of the data bases because some of them were relational with a predetermined scheme, which would have limited us. Whereas NOSQL databases have difficulty analyzing free text as only Elastic knows how to do

# Event Streaming 
Chosen tech – Kafka 
Distributed message management service that allows our infrastructure to withstand extreme changes in the number of messages or in situations where the customer who receives the messages is not available to receive the messages due to congestion or has fallen for one reason or another.
this service is free no charge 
Alternative tools - Pub-Sub /Kinesis:
SAAS services provided by various cloud service providers.
Given our first choice of local FILE SYSTEM we preferred to use a free provider
Computing processing
Chosen tech – computing service, Spark:
Framework that supports the distributed and fast calculation of large amounts of information in real time using cluster resource memory. Supports the processing of large amounts of information for free and has a natural integration with the most common Big Data tools on the market and the tools we have chosen in particular.
Alternative tools – Pandas/hive:
We considered as an alternative to processing our information in the project in real time, but the above alternative was rejected due to the run slowly (epically hive) and limitations of the technology to run under the memory resources of the machine that the above technology ran on.

# Visualization tool 
Chosen tech – Kibana
A visualization tool that is naturally part of elastic stack and allows us to analyze data based on the data we have at Elasticsearch
Alternative tools – Power BI/ google looker / quick sight:
We sculpted the other tools for several reasons - they include a fixed monthly cost and Elastic also has the advantage that KIBANA is an integral part of ELASTIC STACK




# Proposed High-Level Technological Components Overview + Diagram
Extract from the last day all the articles
Daily schedule process that extracts the last day of articles from the Guardian 
and save it in HDFS
Extract what was the most popular word for the last day
Spark process
in order to do so, we clean some data from the articles to distill them.
Select only the relevant column from the entire available columns and convert it to RDD
Remove all the chars that are not alphabetic 
Lower and split all the words to a RDD list 
Group by each word and count them 
Remove all the stop words – using an external file that stores all the stop words.
Sort and keep only the top 1 word.
Add new column of date restructure the top 1 word and data into DF and save it.
Sentiment analysis
Use text blob with spark in order to provide sentiment analysis for each article. Save the outcome in 2 places – HDFS & Elasticsearch DB
Average sentiment
Calculate and aggregate what is the average sentiment for the top word
Twitter
Create producer that listen to tweets that contains the top words 
send the outputs to Kafka topic.
Sentiment analysis – Pyspark that consume the tweets from the last bullet and extract the sentiment for each tweet by text blob.
send the outputs to Kafka topic.
Save the tweets & sentiment in Elastic DB – Pyspark process that consume the tweets & sentiments from the last bullet and save the data in Elastic DB.




# Monitoring 
Create a process that send alerts thought email once a day:
Most common word and its average sentiment.
Headlines of the most positive/negative articles 
Links for the most positive/negative articles
Those alerts will be stored in MySQL following this structure:
Time stamp
Alert name
details

# Reporting / Searching capabilities (Back End)
# Kibana 
connect to the data we stored in Elastic DB and present it in a report the following KPIs:
Top word by day & sentiment regarding news articles 
Tweets average sentiments and trend over time 


