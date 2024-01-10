from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

#sparkContext is not used because we are not using RDD
#inferSchema will let the Spark decide the datatypes of the dataframe
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

#select
print("Let's display the name column:")
people.select("name").show()

#group by count
print("Group by age")
people.groupBy("age").avg("friends").show()

spark.stop()