from kafka import KafkaConsumer
from time import sleep
import json

def connect(topic, server):
    consumer = KafkaConsumer(
        topic,
        group_id='my-group',
        bootstrap_servers=server,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer = lambda m: json.loads(m.decode('ascii')),
        consumer_timeout_ms = 1000)
    return consumer

def consume(consumer, SLEEP_TIME):
    # Consume messages
    print("###############READ################")
    for message in consumer:
        print("Received message: "+str(message))
        #sleep(SLEEP_TIME)
    consumer.commit()


