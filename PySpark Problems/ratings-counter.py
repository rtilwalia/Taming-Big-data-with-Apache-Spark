from pyspark import SparkConf, SparkContext
import collections

#initializing the spark objects
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
##setMaster('local') - running program locally and not on any other machines
##setAppName() - Setting the application name
sc = SparkContext(conf = conf)

#lines will take each line of the data
lines = sc.textFile("C:/spark_course/ml-100k/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

#python function on collections
sortedResults = collections.OrderedDict(sorted(result.items()))

#converting in a key value pair for o/p
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
