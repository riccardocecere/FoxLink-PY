import kafka_interface
from kafka import KafkaConsumer
import os
from time import sleep

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TOPIC_INPUT = os.environ.get('TOPIC_INPUT')
TOPIC_OUTPUT = os.environ.get('TOPIC_OUTPUT')
MESSAGES_PER_SECOND = float(os.environ.get('MESSAGES_PER_SECOND'))
SLEEP_TIME = 1 / MESSAGES_PER_SECOND


def main():
    print('Running Consumer')
    try:
        consumer = kafka_interface.connectConsumer(TOPIC_INPUT, KAFKA_BROKER_URL)
        print("Consumer connected")
    except Exception as ex:
        print("Error connecting kafka broker as Consumer")
        print(ex)

    working = True
    while working:
        message_dict = kafka_interface.consume(consumer)
        #message_dict = consumer.poll(timeout_ms=10, max_records = 5)
        #print(message_dict)
        if (message_dict != {}):
            for topic, messages in message_dict.items():
                print('Messages arrived from topic: ' + str(topic))
                for message in messages:
                    print(message.value)


if __name__ == '__main__':
    main()

