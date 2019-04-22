from pyspark import SparkContext, SparkConf
import pyspark
import operator

print(pyspark.__version__)


def main():

    with pyspark.SparkContext("local", "PySparkWordCount") as sc:
        lines = sc.textFile("Marvel.txt")
        words = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))
        counts = words.reduceByKey(operator.add)
        sorted_counts = counts.sortBy(lambda x: x[1], False)
        for word, count in sorted_counts.toLocalIterator():
            print(u"{} --> {}".format(word, count))


if __name__ == "__main__":
    main()
