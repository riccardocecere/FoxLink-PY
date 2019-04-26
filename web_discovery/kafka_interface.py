import json
from kafka import KafkaProducer, KafkaClient, SimpleProducer
from time import sleep

def connect(server):
    producer=None
    try:
        #producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL, api_version=(0,10), value_serializer=lambda m: dumps(m).encode('ascii'))
        producer = KafkaProducer(bootstrap_servers=server,
                                 api_version=(0, 10),
                                 value_serializer=lambda value: json.dumps(value).encode())
        print("Connected successfully")
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return producer


def send_message(producer, topic_name, key, value):
    # produce json messages
    try:
        future = producer.send(topic = topic_name, key = key, value = value)
        result = future.get(timeout=60)
        print('Message sent successfully')
        print("Message sent: " + str(key) + "-" + str(value))

    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def send_messages(producer, topic_name, SLEEP_TIME, message_set):
    # produce json messages
    i=0
    for elem in message_set:
        try:
            future = producer.send(topic_name, value = str(elem))
            result = future.get(timeout=60)
            print('Message sent successfully')
            print("Message sent: " + str(i) + "-" + str(elem))
            sleep(SLEEP_TIME)

        except Exception as ex:
            print('Exception in publishing message')
            print(str(ex))
        i+=1