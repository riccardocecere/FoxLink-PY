from kafka import KafkaConsumer, KafkaProducer
from time import sleep
import json


def connectConsumer(topic, server):
    consumer = None
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=server,
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
        producer = KafkaProducer(bootstrap_servers=server,
                                 value_serializer=lambda value: json.dumps(value).encode())
    except Exception as ex:
        print('Exception while connecting Kafka broker')
        print(str(ex))
    finally:
        return producer

def consume(consumer):
    # Consume messages
    messages_dict = {}
    try:
        messages_dict = consumer.poll(timeout_ms=10, max_records=5)
    except Exception as ex:
        print('Exception while polling from Kafka broker')
        print(str(ex))
    return messages_dict

def send_message(producer, topic, key, message):
    # produce json messages
    try:
        future = producer.send(topic, value = message)
        result = future.get(timeout=60)
        print('Message sent successfully')
        print("Message sent: " + str(key) + str(message))
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))




