from kafka import KafkaConsumer, KafkaProducer
from time import sleep
import json
import os

TIMEOUT_POLLING = int(os.environ.get('TIMEOUT_POLLING'))
MAX_RECORD_POLLING = int(os.environ.get('MAX_RECORD_POLLING'))

def connectConsumer(topic, server, group = None):
    consumer = None
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=server,
            group_id = group,
            value_deserializer=lambda value: json.loads(value),
            enable_auto_commit=False,
            auto_offset_reset='earliest'
        )
    except Exception as ex:
        print('Exception while connecting Kafka broker')
        print(str(ex))
    finally:
        return consumer

def connectProducer(server):
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=server,
            # Encode all values as JSON
            value_serializer=lambda value: json.dumps(value).encode('utf-8'))
        return producer
    except Exception as ex:
        print('Exception while connecting Kafka broker')
        print(str(ex))

def consume(consumer):
    # Consume messages
    messages_dict = {}
    try:
        messages_dict = consumer.poll(timeout_ms=TIMEOUT_POLLING, max_records=MAX_RECORD_POLLING)
    except Exception as ex:
        print('Exception while polling from Kafka broker')
        print(str(ex))
    return messages_dict

def send_message(producer, topic, value):
    # produce json messages
    try:
        future = producer.send(topic, value = value)
        result = future.get(timeout=60)
        print('Message sent successfully')
        #print("Message sent: " + str(value))
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))




