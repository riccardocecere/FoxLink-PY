import json
import web_discovery_searx as searx
import kafka_interface as kafka
from time import sleep
import os

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TOPIC = os.environ.get('OUTPUT_TOPIC')
MESSAGES_PER_SECOND = float(os.environ.get('MESSAGES_PER_SECOND'))
SLEEP_TIME = 1 / MESSAGES_PER_SECOND

def main():
    f = open('./config.json', 'r')
    config = json.loads(f.read())
    f.close()

    '''searcing sites from sesult pages starting from keyword in batch'''
    sites = searx.web_discovery_with_searx(config['web_discovery']['id_seed_path'],
                                           config['web_discovery']['searx']['num_of_searx_result_pages'])


    '''delete eventual duplicates'''
    sites_set = set()
    for elem in sites:
        sites_set.add(elem)
    print('Running producer')

    '''connect to the broker'''
    try:
        message_producer = kafka.connect(KAFKA_BROKER_URL)
    except Exception as ex:
        print(ex)

    '''sending resulting sites to the next phase'''
    kafka.send_messages(message_producer,TOPIC, SLEEP_TIME, sites_set)
    # i=0
    # for elem in sites_set:
    #     try:
    #         kafka.send_message(message_producer,TOPIC,i,str(elem))
    #         sleep(SLEEP_TIME)
    #     except Exception as ex:
    #         print(ex)
    #     i+=1

    print('Stop producer')

if __name__ == "__main__":
    main()