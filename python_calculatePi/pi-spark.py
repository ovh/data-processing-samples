from time import time
import numpy as np
from random import random
from operator import add

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('CalculatePi').getOrCreate()
sc = spark.sparkContext

# number of points that will be picked in the range [0,1]²
# set to 10 million
n_points = 10000000


def is_point_inside_unit_circle(p):
    # p is useless here
    x, y = random(), random()
    return 1 if x*x + y*y < 1 else 0


t_0 = time()

# parallelize creates a spark Resilient Distributed Dataset (RDD)
# its values are useless in this case
# but allows us to distribute our calculation (inside function)
# The following lines are adapted from  the official spark examples available here:
# http://spark.apache.org/examples.html
count = sc.parallelize(range(0, n_points)) \
             .map(is_point_inside_unit_circle).reduce(add)
print(np.round(time()-t_0, 3), "seconds elapsed for spark approach and n=", n_points)
print("Pi is roughly %f" % (4.0 * count / n_points))

# VERY important to stop SparkSession
# Otherwise, the job will keep running indefinitely
spark.stop()
