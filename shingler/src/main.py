# destination/main.py
import os
import json
import kafka_interface as kafka
import shingler
from bs4 import BeautifulSoup

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
INPUT_TOPIC = os.environ.get('TOPIC_INPUT')
SHINGLE_WINDOW = int(os.environ.get('SHINGLE_WINDOW'))


if __name__ == '__main__':
    consumer = kafka.connectConsumer(topic = INPUT_TOPIC, server = KAFKA_BROKER_URL)

    working = True
    while working:
        i=0
        dict = kafka.consume(consumer = consumer)
        if(dict != {}):
            for topic, messages in dict.items():
                for message in messages:
                    body = BeautifulSoup(message.value['html_raw_text'], 'html.parser')
                    shingle_vector = shingler.compute_shingle_vector(body, SHINGLE_WINDOW)
                    message.value['shingle_vector'] = str(shingle_vector)
                    print('Received message: ' + message.value['url_page'])
                    print('Shingle Vector: '+str(message.value['shingle_vector']))
                    i+=1
        #print('passo')