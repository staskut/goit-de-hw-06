from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
from pyspark.sql import SparkSession
from configs import kafka_config
import os

# Пакет, необхідний для читання Kafka зі Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'
os.environ['SPARK_HOME'] = '/opt/homebrew/Cellar/apache-spark/3.5.4/libexec'
os.environ["JAVA_HOME"] = "/opt/homebrew/Cellar/openjdk@17/17.0.13/libexec/openjdk.jdk/Contents/Home"

# Створення SparkSession
spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .getOrCreate())

# Читання потоку даних із Kafka
# Вказівки, як саме ми будемо під'єднуватися, паролі, протоколи
# maxOffsetsPerTrigger - будемо читати 5 записів за 1 тригер.
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", "staskut_building_sensors") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "5") \
    .load()

# Convert Kafka value from binary to string
messages_df = df.selectExpr("CAST(value AS STRING) as message")

# Define regex pattern to extract values
pattern = r"Time: ([\d\-:\s]+), Sensor ID: (\d+), Temperature: (\d+), Humidity: (\d+)"

# Extract fields using regexp_extract
parsed_df = messages_df \
    .withColumn("timestamp", regexp_extract(col("message"), pattern, 1)) \
    .withColumn("sensor_id", regexp_extract(col("message"), pattern, 2).cast(IntegerType())) \
    .withColumn("temperature", regexp_extract(col("message"), pattern, 3).cast(IntegerType())) \
    .withColumn("humidity", regexp_extract(col("message"), pattern, 4).cast(IntegerType())) \
    .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
    .drop("message")

# Load alert conditions CSV
alerts_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("humidity_min", IntegerType(), True),
    StructField("humidity_max", IntegerType(), True),
    StructField("temperature_min", IntegerType(), True),
    StructField("temperature_max", IntegerType(), True),
    StructField("code", IntegerType(), True),
    StructField("message", StringType(), True)
])

alerts_df = spark.read.csv("alerts_conditions.csv", header=True, schema=alerts_schema)

# Apply watermark and sliding window aggregation
aggregated_df = parsed_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds"), col("sensor_id")) \
    .agg(
    avg("temperature").alias("avg_temperature"),
    avg("humidity").alias("avg_humidity")
) \
    .selectExpr(
    "sensor_id",
    "window.start as window_start",
    "window.end as window_end",
    "avg_temperature",
    "avg_humidity",
)

# Join aggregated data with alerts conditions
alerts_applied_df = aggregated_df.crossJoin(alerts_df) \
    .filter(
    (col("humidity_min") == -999) | (col("avg_humidity") >= col("humidity_min"))
).filter(
    (col("humidity_max") == -999) | (col("avg_humidity") <= col("humidity_max"))
).filter(
    (col("temperature_min") == -999) | (col("avg_temperature") >= col("temperature_min"))
).filter(
    (col("temperature_max") == -999) | (col("avg_temperature") <= col("temperature_max"))
) \
    .select(
    col("sensor_id"),
    date_format(col("window_start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
    date_format(col("window_end"), "yyyy-MM-dd HH:mm:ss").alias("window_end"),
    col("avg_temperature"),
    col("avg_humidity"),
    col("code").alias("alert_code"),
    col("message").alias("alert_message")
)

# Debugging: Print aggregated sensor data
debug_query = alerts_applied_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

alerts_json_df = alerts_applied_df.withColumn("key", col("sensor_id").cast(StringType())) \
    .withColumn("value", to_json(struct(
    col("sensor_id"),
    col("window_start"),
    col("window_end"),
    col("avg_temperature"),
    col("avg_humidity"),
    col("alert_code"),
    col("alert_message")
)))

query = alerts_json_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("topic", "staskut_temperature_humidity_alerts") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint") \
    .option("kafka.security.protocol", kafka_config["security_protocol"]) \
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"]) \
    .option("kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username='{kafka_config['username']}' "
            f"password='{kafka_config['password']}';") \
    .outputMode("append") \
    .start()
query.awaitTermination()
