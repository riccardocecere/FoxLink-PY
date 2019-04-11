from time import sleep
from json import dumps
from kafka import KafkaProducer, KafkaClient, SimpleProducer


def connect(KAFKA_BROKER_URL):
    producer=None
    try:
        #producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL, api_version=(0,10), value_serializer=lambda m: dumps(m).encode('ascii'))
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL,
                                 value_serializer=lambda m: dumps(m).encode('ascii'))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return producer


def send_message(producer, topic_name, key, value):
    # produce json messages
    try:
        producer.send(topic_name, {key: value})
        print('Message sent successfully')
        print("Message sent: " + str(key) + "-" + str(value))
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))