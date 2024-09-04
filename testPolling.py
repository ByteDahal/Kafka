from config import *
from datetime import datetime, timedelta
import pyspark.sql.functions as F
import time


last_timestamp = datetime.now() - timedelta(minutes=5)  # Initialize to a past time

while True:
    source_df = ("fc_monitor_txn_bkp", "ztest")

    destination_df = ("fc_monitor_txn_bkp2", "ztest")

    joined_df = source_df.join(destination_df, "txn_date", "left_anti")

    if joined_df.count() > 1:
        #call the function that appends the data into the database
        pass

    updated_df =  source_df.filter(F.col("last_modified") > last_timestamp)

    if updated_df.count() > 1:
        #update destination_table set all columns value to source_table columns value where txn_date in txn_date of updated_df
        pass

    last_timestamp = datetime.now()

    time.sleep(60)