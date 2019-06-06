from kafka import KafkaConsumer, KafkaProducer
import json
import os
from kafka.partitioner import RoundRobinPartitioner
from kafka import TopicPartition

TIMEOUT_POLLING = int(os.environ.get('TIMEOUT_POLLING_MS'))
MAX_RECORD_POLLING = int(os.environ.get('MAX_RECORD_POLLING'))

def connectSimpleConsumer(server):
    consumer = None
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=server
        )
    except Exception as ex:
        print('Exception while connecting Kafka broker')
        print(str(ex))
    finally:
        return consumer

def connectConsumer(topic, server):
    consumer = None
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=server,
            value_deserializer=lambda value: json.loads(value.decode('utf-8')),
            enable_auto_commit=False,
            auto_offset_reset='earliest'
        )
    except Exception as ex:
        print('Exception while connecting Kafka broker')
        print(str(ex))
    finally:
        return consumer

def connectSimpleProducer(server):
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=server)
    except Exception as ex:
        print('Exception while connecting Kafka broker')
        print(str(ex))
    finally:
        return producer


def connectProducer(server, partitioner = None):
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=server,
                                 value_serializer=lambda value: json.dumps(value).encode('utf-8'),
                                 partitioner = partitioner)
    except Exception as ex:
        print('Exception while connecting Kafka broker')
        print(str(ex))
    finally:
        return producer

def consume(consumer):
    # Consume messages
    messages_dict = {}
    try:
        messages_dict = consumer.poll(timeout_ms=TIMEOUT_POLLING, max_records=MAX_RECORD_POLLING)
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

def get_RoundRobin_partitioner_for_topic(topic, server):
    simple_producer = connectSimpleProducer(server)
    parts = simple_producer.partitions_for(topic)
    partitions = [TopicPartition(topic, p) for p in parts]
    partitioner = RoundRobinPartitioner(partitions=partitions)

    return partitioner




