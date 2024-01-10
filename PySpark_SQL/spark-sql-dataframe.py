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

#where
print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

#group by 
print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

spark.stop()

