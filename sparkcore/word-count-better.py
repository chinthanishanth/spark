import re
from pyspark import SparkConf, SparkContext


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

"""
NOTE:
countByKey: is implemented with reduce, which means that the driver will collect the partial results of the partitions and does the merge itself.
If your result is large, then the driver will have to merge a large number of large dictionaries, which will make the driver crazy.

reduceByKey: map with reduceByKey shuffles the keys to different executors and does the reduction in every worker, so it is more favorable if the data is large.

In conclusion: when your data is large, use map, reduceByKey and collect will make your driver much happier. 
If your data is small, countByKey will introduce less network traffic (one less stage).
"""

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
