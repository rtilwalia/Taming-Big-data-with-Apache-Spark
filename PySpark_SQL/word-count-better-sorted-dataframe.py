from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of my book into a dataframe
inputDF = spark.read.text("C:/spark_course/data/Book")

# Split using a regular expression that extracts words
# the column name of the dataframe in line 7 is 'value' because we haven't named it
# W+ - means split it into words
# alias.("word") - will give the new col name
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))

#cleaning the data
wordsWithoutEmptyString = words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results.
# to show the complete results and not just default 20 - mention show(wordCountsSorted.count()) accordingly
wordCountsSorted.show(wordCountsSorted.count())
