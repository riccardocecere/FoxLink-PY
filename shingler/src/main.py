# destination/main.py
import os
import json
import kafka_interface as kafka
import shingler
from bs4 import BeautifulSoup
import mongodb_interface as mongo

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TOPIC_INPUT_PAGES = os.environ.get('TOPIC_INPUT_PAGES')
TOPIC_INPUT_DOMAINS = os.environ.get('TOPIC_INPUT_DOMAINS')
TOPIC_OUTPUT = os.environ.get('TOPIC_OUTPUT')
SHINGLE_WINDOW = int(os.environ.get('SHINGLE_WINDOW'))


if __name__ == '__main__':
    consumer_pages = kafka.connectConsumer(topic = TOPIC_INPUT_PAGES, server = KAFKA_BROKER_URL)
    consumer_domains = kafka.connectConsumer(topic = TOPIC_INPUT_DOMAINS, server = KAFKA_BROKER_URL)
    producer = kafka.connectProducer(server = KAFKA_BROKER_URL)
    working = True
    while working:
        dict_pages = kafka.consume(consumer = consumer_pages)
        dict_domains = kafka.consume(consumer = consumer_domains)
        if(dict_pages != {}):
            for topic, messages in dict_pages.items():
                for message in messages:
                    body = BeautifulSoup(message.value['html_raw_text'], 'html.parser')
                    shingle_vector = shingler.compute_shingle_vector(body, SHINGLE_WINDOW)
                    message.value['shingle_vector'] = str(shingle_vector)
                    mongo.put(message.value['domain'], json.dumps(message.value))
                    print('Received message: ' + str(message.value['url_page'])+' from spider: '+str(message.value['spider_id']))
                    print('Shingle Vector: '+str(message.value['shingle_vector']))
        if (dict_domains != {}):
            for topic, messages in dict_domains.items():
                for message in messages:
                    print('Received message domain crawled: '+str(message.value)+' from spider: '+str(message.value['spider_id']))
                    kafka.send_message(producer = producer, topic = TOPIC_OUTPUT, value = message.value)
                    print('Sent message for domain: '+str(message.value))