from pyspark import SparkContext, SparkConf
import pyspark

print(pyspark.__version__)
if __name__ == "__main__":
    conf = SparkConf().setAppName("word Count").setMaster("local[3]")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("Ari.txt")

    words = lines.flatMap(lambda line: line.split("|"))

    wordCounts = words.countByValue()

    for word, count in wordCounts.items():
        print("{} : {}".format(word, count))