# destination/main.py
import os
import json
import kafka_interface as kafka
import mongodb_interface as mongo
import structural_clustering as clustering

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TOPIC_INPUT = os.environ.get('TOPIC_INPUT')
TOPIC_OUTPUT = os.environ.get('TOPIC_OUTPUT')
DATABASE_READ = os.environ.get('DATABASE_READ')
DATABASE_WRITE = os.environ.get('DATABASE_WRITE')
threshold = int(os.environ.get('MIN_SIZE_CLUSTER'))

if __name__ == '__main__':
    consumer = kafka.connectConsumer(topic = TOPIC_INPUT, server = KAFKA_BROKER_URL)
    producer = kafka.connectProducer(server=KAFKA_BROKER_URL)
    working = True
    while working:
        dict = kafka.consume(consumer = consumer)
        if(dict != {}):
            for topic, messages in dict.items():
                for message in messages:
                    print('Received message: '+str(message.value['domain']))
                    try:
                        try:
                            collection_name = message.value['domain']
                            collection = mongo.get_collection_from_db(DATABASE_READ, collection_name)
                        except:
                            print('#########################################')
                            print('#######ERROR tryng to read from db#######')
                            print('#########################################')

                        try:
                            clusters = clustering.structural_clustering(collection, threshold)
                        except:
                            print('#########################################')
                            print('########ERROR tryng to cluster###########')
                            print('#########################################')
                        content = {'domain': collection_name,
                                   'clusters': clusters}
                        content_json = json.dumps(content)
                        mongo.put(collection_name, content_json)
                        print('#########################################')
                        print('############Data saved on DB#############')
                        print('#########################################')
                        kafka.send_message(producer = producer, topic=TOPIC_OUTPUT, value = content)
                        print('Sent message: '+str(content))
                    except:
                        print('#############ERROR clusterizer ##############')

        #print('passo')