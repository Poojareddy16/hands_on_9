from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# ----------------------------------------------------------
# Task 1: Basic Streaming Ingestion and JSON Parsing
# ----------------------------------------------------------

spark = SparkSession.builder.appName("L9-Task1-IngestParse").getOrCreate()

schema = StructType([
    StructField("trip_id", IntegerType()),
    StructField("driver_id", IntegerType()),
    StructField("distance_km", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("timestamp", StringType())
])

# Read from the socket server started by data_generator.py
raw_df = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# Parse JSON and extract columns
parsed_df = raw_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Output parsed stream to console
query = (
    parsed_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
