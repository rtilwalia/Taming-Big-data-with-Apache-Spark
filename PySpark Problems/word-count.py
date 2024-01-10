from pyspark import SparkConf, SparkContext
#from pyspark.sql.functions import countByValue

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

inputs = sc.textFile("C:/spark_course/Book") #‪C:\spark_course\Book
words = inputs.flatMap(lambda x: x.split()) #using flatmap and splitting each word from the file
wordCounts = words.countByValue() #counting the occurance of every word in the book

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore') #ignoring the conversion errors 
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
        