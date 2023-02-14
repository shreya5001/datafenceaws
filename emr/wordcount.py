import sys
from pyspark import SparkContext, SparkConf
from operator import add

conf = (SparkConf()
         .setMaster("local")
         .setAppName("WordCounter")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)

if __name__ == "__main__":
    try:
        words = sc.textFile("s3://aws-analytics-course/temp/const.txt").flatMap(lambda line: line.split(" "))
        wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
        wordCounts.saveAsTextFile("s3://aws-analytics-course/temp/results")
    except:
        print("Error")
