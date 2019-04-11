import kafka_interface
from kafka import KafkaConsumer
import os
# ...
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TOPIC_INPUT = os.environ.get('TOPIC_INPUT')
MESSAGES_PER_SECOND = float(os.environ.get('MESSAGES_PER_SECOND'))
SLEEP_TIME = 1 / MESSAGES_PER_SECOND



print('Running Consumer')
try:
    consumer = kafka_interface.connect(TOPIC_INPUT, KAFKA_BROKER_URL)
    print("Consumer connected")
except Exception as ex:
    print("ERROR: " + str(ex))
try:
    kafka_interface.consume(consumer, SLEEP_TIME)
except Exception as ex:
    print("ERROR: "+str(ex))
print("RECEIVED ALL MESSAGE")
