from pyspark.sq import SparkSession

spark = SparkSession.builder \
    .appName("OrionAnalysis") \
    .getOrCreate()

data = [("A", 10), ("B", 20)]
spark.createDataFrame(data, ["key", "value"]).show()

spark.stop()
