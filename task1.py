from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

spark = SparkSession.builder.appName("L9-Task1-BasicStream").getOrCreate()

schema = StructType([
    StructField("trip_id", IntegerType()),
    StructField("driver_id", IntegerType()),
    StructField("distance_km", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("timestamp", StringType())
])

# Read streaming data from the socket
raw_df = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# Parse JSON and add a proper timestamp column
df = (
    raw_df.select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
)

# Write to CSV output
query = (
    df.writeStream
    .outputMode("append")
    .format("csv")
    .option("path", "outputs/task1")
    .option("checkpointLocation", "outputs/checkpoints/task1")
    .start()
)

query.awaitTermination()
