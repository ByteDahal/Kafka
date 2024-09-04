from pyspark.sql.functions import col, current_timestamp
from testProducer import spark, db_url, db_table, db_properties


# Read initial data
df = spark.readStream \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", db_table) \
    .option("user", db_properties["user"]) \
    .option("password", db_properties["password"]) \
    .option("driver", db_properties["driver"]) \
    .load()

# Track the last processed timestamp
last_timestamp = None

def process_batch(batch_df, batch_id):
    global last_timestamp
    if last_timestamp:
        filtered_df = batch_df.filter(col("last_modified") > last_timestamp)
    else:
        filtered_df = batch_df
    
    # Perform your transformations and actions here
    filtered_df.show()  # or write to another sink
    
    last_timestamp = filtered_df.agg({"last_modified": "max"}).collect()[0][0]

query = df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
