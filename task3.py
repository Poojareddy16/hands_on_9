from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, sum as _sum
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

spark = SparkSession.builder.appName("L9-Task3-WindowedAnalytics").getOrCreate()

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

parsed_df = (
    raw_df.select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
)

windowed_df = (
    parsed_df.withWatermark("event_time", "1 minute")
    .groupBy(window(col("event_time"), "5 minutes", "1 minute"))
    .agg(_sum("fare_amount").alias("total_fare"))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_fare")
    )
)

query = (
    windowed_df.writeStream
    .outputMode("append")
    .format("csv")
    .option("path", "outputs/task3")
    .option("checkpointLocation", "outputs/checkpoints/task3")
    .start()
)


query.awaitTermination()
