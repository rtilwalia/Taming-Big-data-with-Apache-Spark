from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("C:/spark_course/data/Marvel-names")

# the whole file will be converted to a column names value
lines = spark.read.text("C:/spark_course/data/Marvel-graph")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.

#func.split() will convert the first value of the row to the heroID
#func.size() will calculate the size of connections with the col1 heroID
#this will give the num of connections with the heroId (-1) to subtract the main heroID
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
    
#top1 most famous hero
mostPopular = connections.sort(func.col("connections").desc()).first()

#finding the name of the superhero from the marvel-names text
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

