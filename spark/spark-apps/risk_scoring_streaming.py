from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml import PipelineModel

# ============ 1) SparkSession ============
spark = (
    SparkSession.builder
    .appName("risk-scoring-streaming")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)


spark.sparkContext.setLogLevel("WARN")

# ============ 2) Kafka message schema (JSON) ============
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("fuel_consumption_rate", DoubleType(), True),
    StructField("eta_variation_hours", DoubleType(), True),
    StructField("traffic_congestion_level", DoubleType(), True),
    StructField("weather_condition_severity", DoubleType(), True),
    StructField("lead_time_days", DoubleType(), True),
    StructField("iot_temperature", DoubleType(), True),
    StructField("route_risk_level", DoubleType(), True),
    StructField("customs_clearance_time", DoubleType(), True),
    StructField("driver_behavior_score", DoubleType(), True),
    StructField("fatigue_monitoring_score", DoubleType(), True),
])

feature_cols = [
    "fuel_consumption_rate",
    "eta_variation_hours",
    "traffic_congestion_level",
    "weather_condition_severity",
    "lead_time_days",
    "iot_temperature",
    "route_risk_level",
    "customs_clearance_time",
    "driver_behavior_score",
    "fatigue_monitoring_score",
]

# ============ 3) Charger le modèle ============
MODEL_PATH = "s3a://delta/models/delivery_time_deviation/RandomForest_20251223_014020"
model = PipelineModel.load(MODEL_PATH)

# ============ 4) Lire Kafka ============
KAFKA_BOOTSTRAP = "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092"
TOPIC = "logistics-events"

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# ============ 5) Parser JSON ============
events = (
    raw.select(from_json(col("value").cast("string"), schema).alias("data"))
       .select("data.*")
       .withColumn("scoring_time", current_timestamp())
)

# ✅ IMPORTANT: éviter que des nulls cassent le modèle
events = events.dropna(subset=feature_cols)

debug_q = (events.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

# ============ 6) Prédire ============
pred = model.transform(events)
pred = pred.withColumnRenamed("prediction", "delivery_deviation_pred")

# ============ 7) risk_score + classification ============
pred = pred.withColumn(
    "risk_score",
    when(col("delivery_deviation_pred") >= 8, 0.95)
    .when(col("delivery_deviation_pred") >= 5, 0.80)
    .when(col("delivery_deviation_pred") >= 2, 0.55)
    .otherwise(0.25)
)

pred = pred.withColumn(
    "severity",
    when(col("risk_score") >= 0.8, "HIGH")
    .when(col("risk_score") >= 0.5, "MEDIUM")
    .otherwise("LOW")
)

pred = pred.withColumn(
    "risk_classification",
    when(col("risk_score") >= 0.8, "High Risk")
    .when(col("risk_score") >= 0.5, "Moderate Risk")
    .otherwise("Low Risk")
)

# ============ 8) outputs ============
predictions_out = pred.select(
    "scoring_time", "timestamp",
    *feature_cols,
    "delivery_deviation_pred",
    "risk_score", "severity", "risk_classification"
)

alerts_out = predictions_out.filter(col("severity") == "HIGH")

# ============ 9) WriteStream -> Delta ============
PRED_PATH   = "s3a://delta/predictions/risk_scoring_stream"
ALERTS_PATH = "s3a://delta/alerts/critical_incidents_stream"

pred_query = (predictions_out.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "s3a://delta/checkpoints/risk_scoring_pred_stream")
  .option("mergeSchema", "true")
  .start(PRED_PATH)
)

alerts_query = (alerts_out.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "s3a://delta/checkpoints/risk_scoring_alerts_stream")
  .option("mergeSchema", "true")
  .start(ALERTS_PATH)
)

# ✅ IMPORTANT: afficher les vraies exceptions
try:
    spark.streams.awaitAnyTermination()
finally:
    if pred_query.exception() is not None:
        print("❌ pred_query exception:\n", pred_query.exception())
    if alerts_query.exception() is not None:
        print("❌ alerts_query exception:\n", alerts_query.exception())
