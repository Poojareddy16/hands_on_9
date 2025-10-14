from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, avg
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# ----------------------------------------------------------
# Task 2: Aggregate total fare & average distance per driver
# ----------------------------------------------------------

spark = SparkSession.builder.appName("L9-Task2-Aggregations").getOrCreate()

schema = StructType([
    StructField("trip_id", IntegerType()),
    StructField("driver_id", IntegerType()),
    StructField("distance_km", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("timestamp", StringType())
])

raw_df = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

parsed_df = raw_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Real-time aggregation
agg_df = parsed_df.groupBy("driver_id").agg(
    _sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# Write results continuously to CSV
query = (
    agg_df.writeStream
    .outputMode("complete")
    .format("csv")
    .option("path", "outputs/task_2")
    .option("checkpointLocation", "checkpoints/task_2")
    .start()
)

query.awaitTermination()
