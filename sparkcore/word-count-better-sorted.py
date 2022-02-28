import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(normalizeWords)
print(words.take(10))

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
print(wordCounts.take(10))
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
print(wordCountsSorted.take(10)) # print first 10 elements in RDD
results = wordCountsSorted.collect()
print(results[1:10]) # print first 10 elements in list

# for result in results:
#     count = str(result[0])
#     word = result[1].encode('ascii', 'ignore')
#     if (word):
#         print(word.decode() + ":\t\t" + count)
