import kafka_interface
from kafka import KafkaConsumer
import os
# ...
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TOPIC_INPUT = os.environ.get('TOPIC_INPUT')


print('Running Consumer')
try:
    consumer = kafka_interface.connect(TOPIC_INPUT, KAFKA_BROKER_URL)
    print("Consumer connected")
except Exception as ex:
    print("ERROR: " + str(ex))
try:
    kafka_interface.consume(consumer)
except Exception as ex:
    print("ERROR: "+str(ex))
