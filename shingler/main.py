# destination/main.py
import os
import json
import kafka_interface as kafka
KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
INPUT_TOPIC = os.environ.get('TOPIC_INPUT')


if __name__ == '__main__':
    consumer = kafka.connectConsumer(topic = INPUT_TOPIC, server = KAFKA_BROKER_URL)

    working = True
    while working:
        i=0
        dict = kafka.consume(consumer = consumer)
        if(dict != {}):
            for topic, messages in dict.items():
                for message in messages:
                    print('Received message: ' + message.value['url_page'])
                    i+=1
        #print('passo')