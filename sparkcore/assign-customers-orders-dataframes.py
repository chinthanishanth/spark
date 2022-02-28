from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("CustOrders").getOrCreate()
schema = StructType([
    StructField("cust_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("cost", FloatType(), True)])

df = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")
df.printSchema()

totalCostDF = df.select("cust_id", "cost").groupby("cust_id")\
                .agg(func.round(func.sum("cost"), 2).alias("total_cost"))\
    .sort(func.col("total_cost"))

totalCostDF.show(totalCostDF.count()) # to display all records in console
