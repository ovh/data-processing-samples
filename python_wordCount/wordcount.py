from operator import add
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
import pyspark.sql.functions as f

spark = SparkSession\
  .builder \
  .appName("PythonWordCount") \
  .getOrCreate()


# Read CSV from OVHcloud Object Storage.
# Dataset found on https://github.com/walkerkq/musiclyrics
data = spark.read.format('csv').options(header='true', inferSchema='true') \
  .load('billboard_lyrics_1964-2015.csv')

print('############ CSV extract:')
data.show()


# Count and group word frequencies on the column Lyrics, when splitted by space comma
data.withColumn('word', f.explode(f.split(f.col('Lyrics'), ' '))) \
  .groupBy('word') \
  .count() \
  .sort('count', ascending=False) \
  .show()

# To remove stop words (like "I", "The", ...), we need to provide arrays of words, not strings. 
# Here we use APache Spark Tokenizer to do so.
# We create a new column to push our arrays of words
tokenizer = Tokenizer(inputCol="Lyrics", outputCol="words_token")
tokenized = tokenizer.transform(data).select('Rank','words_token')

print('############ Tokenized data extract:')
tokenized.show()


# Once in arrays, we can use the Apache Spark function StopWordsRemover
# A new column "words_clean" is here as an output
remover = StopWordsRemover(inputCol='words_token', outputCol='words_clean')
data_clean = remover.transform(tokenized).select('Rank', 'words_clean')

print('############ Data Cleaning extract:')
data_clean.show()


# Final step : like on the beginning, we can group again words and sort them by the most used
result = data_clean.withColumn('word', f.explode(f.col('words_clean'))) \
  .groupBy('word') \
  .count().sort('count', ascending=False)

print('############ TOP20 Most used words in Billboard songs are:')
result.show()

# Stop Spark Process
spark.stop()
