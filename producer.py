from kafka import KafkaProducer
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("Kafka Research") \
    .getOrCreate()

db_url = "jdbc:mysql://10.13.189.16:3306/xdax_facts_test"
db_table = "fc_emi_months"
db_properties = {
    "user": "amrit.dahal",
    "password": "ehI2vdk-bTgV380RoeRH",
    "driver": "com.mysql.cj.jdbc.Driver"
}

df = spark.read.jdbc(url=db_url, table=db_table, properties=db_properties)
df = df.select("account_number", "customer_segment_id", "group_id")

KAFKA_TOPIC_NAME_CONS = "topic_kafka_test"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'



if __name__ == "__main__":
    
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS, value_serializer=lambda x: x.encode('utf-8'))
    
    data = df.collect()
       
    message_list = []
    message = None
    
    for message in data:

        
        message_fields_value_list = []
               
        message_fields_value_list.append(message["account_number"])
        message_fields_value_list.append(message["customer_segment_id"])
        message_fields_value_list.append(message["group_id"])

        message = ','.join(str(v) for v in message_fields_value_list)
        print("Message Type: ", type(message))
        print("Message: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)


    print("Kafka Producer Application Completed. ")
      
       
