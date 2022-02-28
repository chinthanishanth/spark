from pyspark.sql import SparkSession
from pyspark.sql import functions as func
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/fakefriends-header.csv")

avgFriends = people.select("age","friends").groupBy("age").avg("friends").sort("age")

avgFriendsFrmt = people.select("age","friends").groupBy("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age")

avgFriends.show()
avgFriendsFrmt.show()
spark.stop()