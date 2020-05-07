from __future__ import print_function
from operator import add
from pyspark.sql import SparkSession

if __name__ == "__main__":

    # create a SparkSession
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    # read the input file directly in the same Swift container than the one that hosts the current script
    # create a rdd that contains lines of the input file
    lines = spark.read.text("wordcount.txt").rdd.map(lambda r: r[0])

    # split lines, extract words and count the number of occurrences for each of them
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)

    # print the result
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    # very important: stop the current session
    spark.stop()
