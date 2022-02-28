from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("file:///SparkCourse/sparkcore/customer-orders.csv")


def parseLines(line):
    splitValues = line.split(",")
    custID = int(splitValues[0])
    cost = float(splitValues[2])
    return (custID, cost)


custTotalCost = input.map(parseLines).reduceByKey(lambda x, y: x + y)
custTotalCostSorted = custTotalCost.map(lambda x: (x[1], x[0])).sortByKey()

for (key,value) in custTotalCostSorted.collect():
    print("%.2f %s" %(key,value))
