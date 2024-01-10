from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amount = float(fields[2])
    return (customerID, amount)

lines = sc.textFile("C:/spark_course/customer-orders.csv")
rdd = lines.map(parseLine) #converting the data one line at a time by calling parseline func

summing = rdd.map(lambda x : (x[0], x[1])).reduceByKey(lambda x,y : x + y)
#map(lambda x : (x[0], x[1])) - mapping the customerids to their amount
#reduceByKey(lambda x,y : x + y) - The x and y represent two values associated with the same key.
#x + y is the operation to combine or reduce those values into a single result.
results = summing.collect()

for result in results:
    print(result)


