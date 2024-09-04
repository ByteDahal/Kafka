from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, split

# Create a Spark session
spark = SparkSession.builder \
    .appName("Kafka Consumer to DataFrame") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

KAFKA_TOPIC_NAME_CONS = "topic_kafka_test"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

# Define the schema for the incoming data
schema = StructType([
    StructField("account_number", StringType(), True),
    StructField("customer_segment_id", StringType(), True),
    StructField("group_id", StringType(), True)
])

# Read data from Kafka into a streaming DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert the value column from Kafka (which is in binary) to a string
df = df.selectExpr("CAST(value AS STRING) as message")

# Split the message into separate columns based on the comma delimiter
df = df.withColumn("account_number", split(col("message"), ",").getItem(0)) \
       .withColumn("customer_segment_id", split(col("message"), ",").getItem(1)) \
       .withColumn("group_id", split(col("message"), ",").getItem(2))

# Drop the original message column since we've split it into specific fields
df = df.drop("message")

# Write the streaming DataFrame into a memory sink table called "kafka_table"
query = df.writeStream \
    .outputMode("append") \
    .format("memory") \
    .queryName("kafka_table") \
    .start()

# Wait for the stream to process some data
query.awaitTermination(10)  # Run the streaming query for a few seconds

# Read the results from the memory table into a batch DataFrame
final_df = spark.sql("SELECT * FROM kafka_table")

# Show the final DataFrame
final_df.show()

# Stop the streaming query
query.stop()
