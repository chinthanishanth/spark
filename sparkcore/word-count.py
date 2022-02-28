from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

"""
NOTE:
map() transformation is used to transform the data into different values, types by returning the same number of records. 
flatMap() transformation is used to transform from one record to multiple records

"""
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
