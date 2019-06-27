# destination/main.py
import os
import json
import kafka_interface as kafka
import link_parser
import mongodb_interface as mongo
import xpath_service as xpath

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
                    domain = str(message.value['domain'])
                    print('Working for domain: '+domain)
                    list_pages = link_parser.check_urls(message.value['filtered_pages'])

                    for dict in list_pages:
                        print('url3: ' +str(dict['referring_url']))
                    xpaths = xpath.find_xpath(domain, list_pages)

                    if xpaths:
                        print('Xpaths elaborated')
                        xpath_generalized = xpath.generalize_xpath(xpaths)
                        if xpath_generalized:
                            print('Generated Xpath Generalized')

                            content = {
                                'domain': domain,
                                'Xpaths': xpaths,
                                'Generalized_Xpaths': xpath_generalized
                            }
                            mongo.put(domain,json.dumps(content))