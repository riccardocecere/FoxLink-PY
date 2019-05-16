import json
import web_discovery_searx as searx
import kafka_interface as kafka
from time import sleep
import os

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TOPIC = os.environ.get('OUTPUT_TOPIC')
MESSAGES_PER_SECOND = float(os.environ.get('MESSAGES_PER_SECOND'))
SLEEP_TIME = 1 / MESSAGES_PER_SECOND
SEED = os.environ.get('SEED')
NUMBER_RESULT_PAGES = int(os.environ.get('NUMBER_RESULT_PAGES'))
SEARX_ADDRESS = os.environ.get('SEARX_ADDRESS')

def main():

    ''' searcing sites from result pages starting from keyword in batch '''
    sites = searx.web_discovery_with_searx(seed = SEED, number_result_pages = NUMBER_RESULT_PAGES, searx_address = SEARX_ADDRESS)

    '''delete eventual duplicates'''
    sites_set = set()
    for elem in sites:
        sites_set.add(elem)
    print('Running producer')

    '''connect to the broker'''
    try:
        message_producer = kafka.connect(server = KAFKA_BROKER_URL)
    except Exception as ex:
        print(ex)

    '''sending resulting sites to the next phase'''
    kafka.send_messages(producer = message_producer,topic = TOPIC, pause = SLEEP_TIME, message_set = sites_set)
    # i=0
    # for elem in sites_set:
    #     try:
    #         kafka.send_message(producer = message_producer,topic = TOPIC,key = i,value = str(elem))
    #         sleep(SLEEP_TIME)
    #     except Exception as ex:
    #         print(ex)
    #     i+=1

    print('Stop producer')

if __name__ == "__main__":
    main()