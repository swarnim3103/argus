from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, count, sum as spark_sum, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

spark = (
    SparkSession.builder
    .appName("ArgusFraudPipeline")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("account_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant", StringType()),
    StructField("category", StringType()),
    StructField("city", StringType()),
    StructField("card_present", BooleanType()),
    StructField("event_time", StringType()),
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp("event_time"))
)

flagged = parsed.withColumn(
    "risk_flag",
    when((col("amount") > 1000), "high_amount")
    .when((~col("card_present")) & (col("amount") > 500), "card_not_present_risk")
    .otherwise("none"),
).filter(col("risk_flag") != "none")

rule_query = (
    flagged.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .queryName("rule_based_flags")
    .start()
)

windowed = (
    parsed
    .withWatermark("event_time", "2 minutes")
    .groupBy(window(col("event_time"), "1 minute"), col("account_id"))
    .agg(count("*").alias("txn_count"), spark_sum("amount").alias("total_amount"))
    .withColumn(
        "velocity_flag",
        when(col("txn_count") >= 3, "velocity_alert").otherwise("none"),
    )
    .filter(col("velocity_flag") != "none")
)

velocity_query = (
    windowed.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .queryName("velocity_flags")
    .start()
)

spark.streams.awaitAnyTermination()