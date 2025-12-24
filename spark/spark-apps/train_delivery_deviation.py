from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.storagelevel import StorageLevel

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from datetime import datetime

# =========================
# Paths (MinIO bucket = delta)
# =========================
DELTA_PATH = "s3a://delta/logistics_data"
MODEL_BASE_PATH = "s3a://delta/models/delivery_time_deviation"

# =========================
# Spark Session (MANDATORY in spark-manager)
# =========================
spark = (
    SparkSession.builder
    .appName("TrainDeliveryTimeDeviation")
    .getOrCreate()
)

# =========================
# Load & prepare
# =========================
df = spark.read.format("delta").load(DELTA_PATH).dropna()

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
label_col = "delivery_time_deviation"

data = df.select(
    *[col(c).cast("double").alias(c) for c in feature_cols],
    col(label_col).cast("double").alias(label_col),
).dropna()

# Cache (important for 3 models)
data = data.repartition(4).persist(StorageLevel.MEMORY_ONLY)
n = data.count()
print("✅ Total rows used:", n)

train, test = data.randomSplit([0.8, 0.2], seed=42)

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# CPU-friendly params (ok for 4 cores)
models = {
    "LinearRegression": LinearRegression(featuresCol="features", labelCol=label_col, maxIter=50),
    "RandomForest": RandomForestRegressor(featuresCol="features", labelCol=label_col, numTrees=40, maxDepth=8, seed=42),
    "GBT": GBTRegressor(featuresCol="features", labelCol=label_col, maxIter=40, maxDepth=5, seed=42),
}

rmse_eval = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse")
mae_eval  = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="mae")
r2_eval   = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="r2")

results = []
best_name, best_model, best_rmse = None, None, float("inf")

for name, reg in models.items():
    pipe = Pipeline(stages=[assembler, reg])
    fitted = pipe.fit(train)
    pred = fitted.transform(test)

    rmse = rmse_eval.evaluate(pred)
    mae  = mae_eval.evaluate(pred)
    r2   = r2_eval.evaluate(pred)

    results.append((name, rmse, mae, r2))
    print(f"➡️ {name}: RMSE={rmse:.4f} MAE={mae:.4f} R2={r2:.4f}")

    if rmse < best_rmse:
        best_rmse = rmse
        best_name = name
        best_model = fitted

print("\n=== Model comparison (sorted by RMSE) ===")
for r in sorted(results, key=lambda x: x[1]):
    print(f"{r[0]:16s}  RMSE={r[1]:.4f}  MAE={r[2]:.4f}  R2={r[3]:.4f}")

print("\n🏆 Best model:", best_name, "RMSE=", best_rmse)

# =========================
# Save best model to MinIO (inside delta bucket)
# =========================
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
save_path = f"{MODEL_BASE_PATH}/{best_name}_{ts}"

best_model.write().overwrite().save(save_path)
print("✅ Saved best PipelineModel to:", save_path)

# Optional: write a small “latest pointer” file (easy for reload)
latest_path = f"{MODEL_BASE_PATH}/LATEST"
spark.createDataFrame([(save_path, ts, best_name, float(best_rmse))],
                      ["model_path", "timestamp", "model_name", "rmse"]) \
     .coalesce(1) \
     .write.mode("overwrite").format("json").save(latest_path)

print("✅ Updated LATEST pointer at:", latest_path)

spark.stop()
