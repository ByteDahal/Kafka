from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Database Streaming") \
    .getOrCreate()

# JDBC properties
db_url = "jdbc:mysql://localhost:3306/your_database"
db_table = "your_table"
db_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Define a function to read from the database
def read_from_db():
    df = spark.read \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", db_table) \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .option("driver", db_properties["driver"]) \
        .load()
    return df

# Use Spark Structured Streaming to periodically poll the database
df = spark.readStream \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", db_table) \
    .option("user", db_properties["user"]) \
    .option("password", db_properties["password"]) \
    .option("driver", db_properties["driver"]) \
    .load()

# Define transformations and output
query = df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
