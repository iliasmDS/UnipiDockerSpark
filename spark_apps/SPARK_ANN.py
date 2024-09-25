import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.feature import BucketedRandomProjectionLSH, VectorAssembler
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, FloatType, StringType, TimestampType
import sys
from time import time

##################################################################

try: memory = f"{int(sys.argv[1])}g"

except: memory = None

try: cores = f"{int(sys.argv[2])}"

except: cores = None

##################################################################

spark = SparkSession.builder \
    .appName("DockerSpark_ANN") \
    .config("spark.executor.cores", cores)\
    .config("spark.executor.memory", memory)\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

##################################################################

data_path = "/opt/spark/data/dummy-data.csv"

query_path = "/opt/spark/data/dummy-queries.csv"

##################################################################

df = spark.read.csv(data_path, header=True)

df = df.selectExpr(*[f"cast({col} as float) as {col}" for col in df.columns])

df_q = spark.read.csv(query_path, header=True)

df_q = df_q.selectExpr(*[f"cast({col} as float) as {col}" for col in df_q.columns])

##################################################################

def vectorize(df):
    """
    Uses the 100 vector features (starting with "f") to make a vector.
    """
    vector_cols = [col for col in df.columns if col.startswith('f')]
    assembler = VectorAssembler(inputCols=vector_cols, outputCol="vectors")
    df_vector = assembler.transform(df).drop(*vector_cols)
    return df_vector

df = vectorize(df)

df_q = vectorize(df_q)

##################################################################

brp = BucketedRandomProjectionLSH(inputCol='vectors', numHashTables = 5, bucketLength = 5)

model = brp.fit(df)

##################################################################

def get_subset(data, row):

    if row.query_type == 1.0:

        return data.filter(F.col("C") == row.v)

    elif row.query_type == 2.0:

        return data.filter((F.col("T") >= row.l) & (F.col("T") <= row.r))

    elif row.query_type == 3.0:

        return data.filter((F.col("C") == row.v) & ((F.col("T") >= row.l) & (F.col("T") <= row.r)))
    
    return data

##################################################################

def perform_ann(row):
    
    subset = get_subset(df, row)
    
    result = model.approxNearestNeighbors(
        dataset = subset,
        key = row.vectors,
        numNearestNeighbors=100,
        distCol='Euclidean_Distance'
    )

    result = result.withColumn('query_vector', F.lit(row.vector))\
        .withColumn('v', F.lit(row.v))\
        .withColumn('l', F.lit(row.l))\
        .withColumn('r', F.lit(row.r))\
        .withColumn('query_type', F.lit(row.query_type))\
        .withColumnRenamed('vector', 'data_vector')\
        .withColumn('current_time', F.date_format(F.current_timestamp(), "HH:mm:ss.SSS")) \
        .select('data_vector', 'query_vector', 'Euclidean_Distance', 'query_type', 'C', 'v', 'l', 'T', 'r')
        
    return result

#################################################################

time_schema = StructType([
    StructField("computation_seconds", FloatType(), True),
    StructField("threshold", FloatType(), True),
    StructField("executor_memory", StringType(), True),
    StructField("executor_cores", StringType(), True),
    StructField("Model", StringType(), True),
])

time_df = spark.createDataFrame([], schema = time_schema)

#################################################################

start_time = time()

for i, row in enumerate(df_q.rdd.collect()):

    result = perform_ann(row)

    if i == 0: 

        result.write.mode("overwrite").parquet(f'/opt/spark/output/ANN.parquet')

    else:

        result.write.mode("append").parquet(f'/opt/spark/output/ANN.parquet')

end_time = time()

computation_seconds = end_time - start_time

time_df = time_df.union(spark.createDataFrame([(computation_seconds, None, memory, cores, 'ANN')], schema = time_schema))

time_df.write.mode("append").parquet(f'/opt/spark/output/Time.parquet')

spark.stop()