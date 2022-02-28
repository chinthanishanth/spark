from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)
words = sc.textFile("file:///sparkcourse/test.txt")

wc = words.map(lambda word: word.split())
print(wc.collect())
