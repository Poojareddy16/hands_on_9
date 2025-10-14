from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, avg as _avg
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

spark = SparkSession.builder.appName("Task2_Aggregations").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Schema for the incoming JSON
schema = StructType([
    StructField("trip_id", IntegerType()),
    StructField("driver_id", IntegerType()),
    StructField("distance_km", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("timestamp", StringType()),
])

# 1) Read from socket
raw_df = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# 2) Parse JSON
parsed_df = raw_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# 3) Cast timestamp and add watermark (good practice for aggregations)
stream_df = (
    parsed_df
    .withColumn("event_time", col("timestamp").cast(TimestampType()))
    .withWatermark("event_time", "1 minute")
)

# 4) Aggregations (driver-level)
agg_df = stream_df.groupBy("driver_id").agg(
    _sum("fare_amount").alias("total_fare"),
    _avg("distance_km").alias("avg_distance")
)

# 5) foreachBatch writer to CSV (works with update mode)
def write_batch(batch_df, batch_id):
    (
        batch_df
        .coalesce(1)                       # optional: fewer files per batch
        .write
        .mode("append")
        .option("header", "true")
        .csv("outputs/task_2")
    )

query = (
    agg_df.writeStream
    .outputMode("update")                   # logical mode for aggregations
    .foreachBatch(write_batch)              # <-- key fix: write each batch as files
    .option("checkpointLocation", "checkpoints/task_2")
    .start()
)

query.awaitTermination()
