import findspark
findspark.init()

from pyspark.sql import SparkSession

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("DockerSpark") \
    .getOrCreate()

# Print Spark session info to verify setup
print(spark)

# Create an RDD with some sample data (a list of numbers)
data = list(range(1, 1001))  # Creates a list of numbers from 1 to 1000

# Distribute the data across the cluster by parallelizing the list into an RDD
rdd = spark.sparkContext.parallelize(data, numSlices=4)  # Adjust `numSlices` for partitioning

# Apply a transformation to square each number
squared_rdd = rdd.map(lambda x: x ** 2)

# Collect the results (which will trigger the computation)
results = squared_rdd.collect()

# Print a few results to verify
print("Squared numbers:", results[:10])

# Check how many partitions are being used (should match the number of workers)
print(f"Number of partitions: {squared_rdd.getNumPartitions()}")

# Stop the Spark session
spark.stop()