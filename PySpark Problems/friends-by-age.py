from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2]) #converting age and friends to num to perform math func
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("C:/spark_course/fakefriends.csv")
rdd = lines.map(parseLine) #converting the data one line at a time by calling parseline func

#mapValues is a transformation- taking value of the k,v pair and (key, (value, 1))
#reduceByKey is an action - summing up the (value,1) pairs acc to the keys
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect() #collect() is an action
for result in results:
    print(result)
