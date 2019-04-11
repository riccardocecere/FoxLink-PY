import json
import web_discovery_searx as searx
import kafka_interface as kafka
from time import sleep
import os

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TOPIC = os.environ.get('TOPIC')
MESSAGES_PER_SECOND = float(os.environ.get('MESSAGES_PER_SECOND'))
SLEEP_TIME = 1 / MESSAGES_PER_SECOND

def main():
    f = open('./config.json', 'r')
    config = json.loads(f.read())
    f.close()
    try:
        sites = searx.web_discovery_with_searx(config['web_discovery']['id_seed_path'],
                                           config['web_discovery']['searx']['num_of_searx_result_pages'],)
    except Exception as ex:
        print("Searx ERROR: "+str(ex))

    sites_set = set()
    for elem in sites:
        sites_set.add(elem)
    print('Running producer')
    #message_producer=kafka.connect_kafka_producer()
    message_producer=kafka.connect(KAFKA_BROKER_URL)
    i=0
    #for elem in sites_set:
    for j in range(0,4):
        try:
            #kafka.send_message(message_producer,topic_name,i,str(elem))
            kafka.send_message(message_producer,TOPIC,j,"BELLA")

            #print("Message sent: "+str(i)+"-"+str(elem))
            sleep(SLEEP_TIME)
        except Exception as ex:
            print(ex)
        i+=1
    try:
        kafka.send_message(message_producer, TOPIC,i,"END MESSAGGE")
        sleep(SLEEP_TIME)
    except Exception as ex:
        print(ex)
    print('Stop producer')

if __name__ == "__main__":
    main()