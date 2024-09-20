import findspark

findspark.init()

from pyspark.sql import SparkSession

import sys

if len(sys.argv) == 2:

    memory = int(sys.argv[1])

else: memory = 4

##################################################################

spark = SparkSession.builder \
    .appName("DockerSpark") \
    .config("spark.executor.memory", f"{memory}g")\
    .config("spark.executor.cores", "4")\
    .config("spark.driver.memory", "2g")\
    .getOrCreate()

data = list(range(1, 1001))

rdd = spark.sparkContext.parallelize(data, numSlices = 3)

squared_rdd = rdd.map(lambda x: x ** 2)

results = squared_rdd.collect()

print("Squared numbers:", results[:10])

print(f"Number of partitions: {squared_rdd.getNumPartitions()}")

spark.stop()