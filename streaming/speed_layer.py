
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    sum as spark_sum,
    avg,
    count,
    approx_count_distinct
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = "charge-events"

OUTPUT_PATH = "/opt/chargepulse/streaming/output_v2"
CHECKPOINT_PATH = "/opt/chargepulse/streaming/checkpoints_v2"


event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("station_id", StringType(), True),
    StructField("connector_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("energy_delta_kwh", DoubleType(), True),
    StructField("power_kw", DoubleType(), True),
    StructField("session_status", StringType(), True)
])


spark = (
    SparkSession.builder
    .appName("ChargePulse-Speed-Layer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)


events = (
    kafka_stream
    .select(
        from_json(
            col("value").cast("string"),
            event_schema
        ).alias("event")
    )
    .select("event.*")
    .withColumn("event_time", to_timestamp("event_ts"))
    .filter(
        col("event_id").isNotNull()
        & col("event_time").isNotNull()
        & col("station_id").isNotNull()
        & col("energy_delta_kwh").isNotNull()
    )
)


deduplicated_events = (
    events
    .withWatermark("event_time", "5 seconds")
    .dropDuplicatesWithinWatermark(["event_id"])
)


speed_metrics = (
    deduplicated_events
    .groupBy(
        window(col("event_time"), "10 seconds"),
        col("station_id")
    )
    .agg(
        spark_sum("energy_delta_kwh").alias("total_energy_kwh"),
        avg("power_kw").alias("average_power_kw"),
        count("*").alias("event_count"),
        approx_count_distinct("session_id").alias("active_sessions")
    )
)


query = (
    speed_metrics.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)


print("ChargePulse Speed Layer is running...")
print("Reading Kafka topic:", KAFKA_TOPIC)

query.awaitTermination()
