import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

#example for re.compile(r'\W+', re.UNICODE)
#text = "Hello, World! This is a Unicode string with some punctuation marks and spaces."
#['Hello', 'World', 'This', 'is', 'a', 'Unicode', 'string', 'with', 'some', 'punctuation', 'marks', 'and', 'spaces', '']
#split(text.lower()) - transforming words to lower case 
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("C:/spark_course/Book")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
