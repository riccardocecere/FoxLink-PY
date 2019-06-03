import kafka_interface as kafka
from kafka import KafkaConsumer
import os
import time
import multiprocessing
import foxlink_crawler

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TOPIC_INPUT = os.environ.get('TOPIC_INPUT')
TOPIC_OUTPUT = os.environ.get('TOPIC_OUTPUT')
MESSAGES_PER_SECOND = float(os.environ.get('MESSAGES_PER_SECOND'))
SLEEP_TIME = 1 / MESSAGES_PER_SECOND
depth_limit = os.environ.get('DEPTH_LIMIT')
download_delay = os.environ.get('DOWNLOAD_DELAY')
closespider_pagecount = os.environ.get('CLOSESPIDER_PAGECOUNT')
autothrottle_enable = os.environ.get('AUTOTHROTTLE_ENABLE')
autothrottle_target_concurrency = os.environ.get('AUTOTHROTTLE_TARGET_CONCURRENCY')
TIMEOUT_CRAWLER = int(os.environ.get('TIMEOUT_CRAWLER'))
CONSUMER_GROUP = os.environ.get('CONSUMER_GROUP')


def main():
    print('Running Consumer')
    try:
        consumer = kafka.connectConsumer(topic = TOPIC_INPUT, server = KAFKA_BROKER_URL, group = CONSUMER_GROUP)
        print("Consumer connected")
    except Exception as ex:
        print("Error connecting kafka broker as Consumer")
        print(ex)
    try:
        producer = kafka.connectProducer(server = KAFKA_BROKER_URL)
        print("Consumer connected")
    except Exception as ex:
        print("Error connecting kafka broker as Consumer")
        print(ex)
    i=0
    working = True
    while working:
        message_dict = kafka.consume(consumer = consumer)
        if (message_dict != {}):
            for topic, messages in message_dict.items():
                urls = []
                for message in messages:
                    print('Received message: ' + str(message.value))
                    urls.append(message.value)
                try:
                    print('Starting new crawling process...')
                    '''inizializing a new thread for crawl use to be stopped after TIMEOUT_CRAWLER passes'''
                    p = multiprocessing.Process(target=foxlink_crawler.intrasite_crawling_iterative, name="Crawler",
                                                args=(urls,depth_limit,download_delay,
                                                      closespider_pagecount,autothrottle_enable,
                                                      autothrottle_target_concurrency))
                    p.start()
                    # Wait n seconds for crawling
                    time.sleep(TIMEOUT_CRAWLER)
                    # Terminate crawling
                    p.terminate()
                    # Cleanup
                    p.join()
                    print('Join process')

                except Exception as ex:
                    print(ex)

if __name__ == '__main__':
    main()

