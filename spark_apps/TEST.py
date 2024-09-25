from pyspark.sql import SparkSession, Row
import socket

spark = SparkSession.builder \
    .appName("TEST") \
    .getOrCreate()

def calc_func(value):

    return value ** 2

def process_partition(partition):

    container_id = socket.gethostname()
    
    return [Row(value=row.value, calculation = calc_func(row.value), container_id = container_id) for row in partition if row]

data = [(i,) for i in range(100000)]

df = spark.createDataFrame(data, ["value"])

splits = df.repartition(100)

results = splits.rdd.mapPartitions(process_partition).collect()

results = spark.createDataFrame(results)

print(results.show())

results.write.mode('overwrite').parquet("output/TEST.parquet")

spark.stop()
