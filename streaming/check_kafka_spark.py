
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

schema = StructType([
    StructField("event_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("station_id", StringType()),
    StructField("connector_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("energy_delta_kwh", DoubleType()),
    StructField("power_kw", DoubleType()),
    StructField("session_status", StringType())
])

spark = SparkSession.builder.appName("ChargePulse-Diagnostic").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "charge-events")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("event")
).select("event.*")

print("RAW KAFKA COUNT:", df.count())
print("PARSED EVENT COUNT:", parsed.count())
parsed.select("event_id", "event_ts", "station_id", "energy_delta_kwh").show(10, False)

spark.stop()
