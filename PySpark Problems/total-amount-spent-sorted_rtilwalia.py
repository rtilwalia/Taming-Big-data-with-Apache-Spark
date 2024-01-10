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

summing = rdd.reduceByKey(lambda x,y : x + y)
#map(lambda x : (x[0], x[1])) - mapping the customerids to their amount
#reduceByKey(lambda x,y : x + y) - The x and y represent two values associated with the same key.
#x + y is the operation to combine or reduce those values into a single result.
flipped = summing.map(lambda  x: (x[1], x[0])).sortByKey()

results = flipped.collect()

for result in results:
    print(result)


