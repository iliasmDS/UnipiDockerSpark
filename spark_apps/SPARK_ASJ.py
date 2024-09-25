import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml.feature import BucketedRandomProjectionLSH, VectorAssembler
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType, TimestampType
import sys
from time import time

##################################################################

try: memory = f"{int(sys.argv[1])}g"

except: memory = None

try: cores = f"{int(sys.argv[2])}"

except: cores = None

##################################################################

spark = SparkSession.builder \
    .appName("DockerSpark_ASJ") \
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

brp = BucketedRandomProjectionLSH(inputCol='vectors', outputCol='hashes', numHashTables = 20, bucketLength = 5)

model = brp.fit(df)

##################################################################

threshold = 30.0 # In a real problem domain knowledge can be used to decide the threshold

similarity_df = model.approxSimilarityJoin(
    datasetA = df,
    datasetB = df_q,
    threshold = threshold,
    distCol = "Euclidean_Distance"
)

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

similarity_df = similarity_df.select(
    F.col('datasetA.vector').alias('data_vector'),
    F.col('datasetB.vector').alias('query_vector'),
    'Euclidean_Distance',
    F.col('datasetB.query_type'),
    F.col('datasetA.C'),
    F.col('datasetB.v'),
    F.col('datasetB.l'),
    F.col('datasetA.T'),
    F.col('datasetB.r'),
)

similarity_df  = similarity_df.filter(
    ((F.col('query_type') == 1.0) & (F.col('C') == F.col('v'))) |
    ((F.col('query_type') == 2.0) & (F.col('T') >= F.col('l')) & (F.col('T') <= F.col('r'))) |
    ((F.col('query_type') == 3.0) & (F.col('C') == F.col('v')) & (F.col('T') >= F.col('l')) & (F.col('T') <= F.col('r')))
                                     )

gb = Window.partitionBy('query_vector').orderBy(F.col('Euclidean_Distance').asc())

similarity_df = similarity_df.withColumn('rank', F.rank().over(gb))

similarity_df = similarity_df.filter(F.col('rank') <= 100).drop('rank')

similarity_df.write.mode("overwrite").parquet(f'/opt/spark/output/ASJ.parquet')

#################################################################

end_time = time()

computation_seconds = end_time - start_time

time_df = time_df.union(spark.createDataFrame([(computation_seconds, threshold, memory, cores, 'ASJ')], schema = time_schema))

time_df.write.mode("append").parquet(f'/opt/spark/output/Time.parquet')

spark.stop()