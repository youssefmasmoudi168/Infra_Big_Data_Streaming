from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# ===============================================================
# 1️⃣ Create Spark Session – all configs come from spark-defaults.conf
# ===============================================================
spark = (
    SparkSession.builder
        .appName("KafkaToDelta")
        .getOrCreate()
)

# ===============================================================
# 2️⃣ Define schema for incoming Kafka JSON messages
# ===============================================================
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("fuel_consumption_rate", DoubleType(), True),
    StructField("eta_variation_hours", DoubleType(), True),
    StructField("traffic_congestion_level", DoubleType(), True),
    StructField("weather_condition_severity", DoubleType(), True),
    StructField("lead_time_days", DoubleType(), True),
    StructField("iot_temperature", DoubleType(), True),
    StructField("cargo_condition_status", StringType(), True),
    StructField("route_risk_level", DoubleType(), True),
    StructField("customs_clearance_time", DoubleType(), True),
    StructField("driver_behavior_score", DoubleType(), True),
    StructField("fatigue_monitoring_score", DoubleType(), True),
    StructField("delivery_time_deviation", DoubleType(), True),
    StructField("delay_probability", DoubleType(), True),
    StructField("risk_classification", StringType(), True),
])

# ===============================================================
# 3️⃣ Read Stream from Kafka
# ===============================================================
kafka_servers = "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092"

df_kafka = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", "logistics-events")
        .option("startingOffsets", "latest")
        .option("kafka.security.protocol", "PLAINTEXT")
        .load()
)

# ===============================================================
# 4️⃣ Parse JSON payload coming from Kafka
# ===============================================================
df_parsed = (
    df_kafka
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
)

# ===============================================================
# 5️⃣ Write Stream to Delta Lake on MinIO (S3A)
# ===============================================================
(
    df_parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "s3a://checkpoints/logistics_data")
        .option("path", "s3a://delta/logistics_data")
        .start()
        .awaitTermination()
)
