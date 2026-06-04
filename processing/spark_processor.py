import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, floor, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Dynamically grab the exact PySpark version installed in the virtual environment
spark_version = pyspark.__version__
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}"

print(f"Initializing Spark Core {spark_version} with Kafka Connector {kafka_package}...")

# 1. Initialize Spark Session with dynamic versioning
spark = SparkSession.builder \
    .appName("CryptoTickProcessor") \
    .config("spark.jars.packages", kafka_package) \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define the exact schema expected from the Kafka JSON payload
json_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# 3. Read the stream from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-ticks") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse and Transform the Data
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), json_schema).alias("data")) \
    .select("data.*")

# Truncate numerical values to exactly 5 decimal places (chop, do not round)
processed_stream = parsed_stream \
    .withColumn("price", floor(col("price") * 100000) / 100000) \
    .withColumn("quantity", floor(col("quantity") * 100000) / 100000) \
    .withColumn("timestamp", to_timestamp(col("timestamp") / 1000))

# 5. Define the PostgreSQL Sink using foreachBatch
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/cryptodb") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "crypto_ticks") \
        .option("user", "admin") \
        .option("password", "adminpassword") \
        .mode("append") \
        .save()
    
    print(f"Batch {batch_id} committed to PostgreSQL.")

# 6. Start the streaming pipeline with an explicit checkpoint directory
query = processed_stream.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "./spark_checkpoints") \
    .start()

query.awaitTermination()