from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType #need to upload these as there is no header in the dataset 

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

#defining the schema using the StructType() and  StructField() function 
#True means that null values are allowed
schema = StructType([ \
                     StructField("customerID", StringType(), True), \
                     StructField("orderID", StringType(), True), \
                     StructField("amount", FloatType(), True)])

# Read the file as dataframe
df = spark.read.schema(schema).csv("C:/spark_course/data/customer-orders.csv")
df.printSchema()

#select only customer and amount
customerBills = df.select("customerID", "amount")

#sum according to the customerId
sumcustomerBills = customerBills.groupBy("customerID").sum("amount")
sumcustomerBills.show()

spark.stop()