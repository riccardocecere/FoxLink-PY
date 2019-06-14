# destination/main.py
import os
import json
import kafka_interface as kafka
import mongodb_interface as mongo

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TOPIC_INPUT = os.environ.get('TOPIC_INPUT')
DATABASE = os.environ.get('DATABASE')

if __name__ == '__main__':
    consumer = kafka.connectConsumer(topic = TOPIC_INPUT, server = KAFKA_BROKER_URL)
    working = True
    while working:
        dict = kafka.consume(consumer = consumer)
        if(dict != {}):
            for topic, messages in dict.items():
                for message in messages:
                    print('Received message: '+str(message.value['domain']))
        #print('passo')