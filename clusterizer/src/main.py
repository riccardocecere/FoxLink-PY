# destination/main.py
import os
import json
import kafka_interface as kafka

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
INPUT_TOPIC_PAGES = os.environ.get('TOPIC_INPUT_PAGES')
INPUT_TOPIC_DOMAINS = os.environ.get('TOPIC_INPUT_DOMAINS')

if __name__ == '__main__':
    consumer_pages = kafka.connectConsumer(topic = INPUT_TOPIC_PAGES, server = KAFKA_BROKER_URL)
    consumer_domains = kafka.connectConsumer(topic = INPUT_TOPIC_DOMAINS, server = KAFKA_BROKER_URL)
    domains_dict = {}
    working = True
    while working:
        dict_pages = kafka.consume(consumer = consumer_pages)
        dict_domains = kafka.consume(consumer = consumer_domains)
        if(dict_pages != {}):
            for topic, messages in dict_pages.items():
                for message in messages:
                    dict_domains[message.value['domain']].append(message.value)
                    print('Received page: '+str(message.value['url_page']))
            if (dict_domains != {}):
                for topic, messages in dict_domains.items():
                    for message in messages:
                        #Todo
                        print('domain: ' + str(message.value['domain']))

        #print('passo')