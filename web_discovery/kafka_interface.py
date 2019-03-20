from time import sleep
from json import dumps
from kafka import KafkaProducer

'''
def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['foxlink_kafka_1:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer
'''
def connect():
    producer=None
    try:
        producer = KafkaProducer(bootstrap_servers=['foxlink_kafka_1:9092'], api_version=(0,10), value_serializer=lambda m: dumps(m).encode('ascii'))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return producer

'''
def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))
'''

def send_message(producer, topic_name, key, value):
    # produce json messages
    try:
        producer.send('json-topic', {'key': 'value'})
        print('Message sent successfully')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))