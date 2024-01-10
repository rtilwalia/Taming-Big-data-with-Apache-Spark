import re
from pyspark import SparkConf, SparkContext

#normalizing text in the book
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("C:/spark_course/Book")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
#map(lambda x: (x, 1)) - using mapper this converts each individual word to (k,v) pair i.e. (word, 1)
#reduceByKey(lambda x, y: x + y) - aggregate together every time that each individual word appeared using the lambda function
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
#now we need to sort the number of time the word has appeared so, swaping the (k,v) pair to (v,k) using lambda func
#wordCounts.map(lambda (x,y) : (y,x)).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
