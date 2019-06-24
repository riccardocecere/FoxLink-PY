# destination/main.py
import os
import json
import kafka_interface as kafka
import mongodb_interface as mongo
import referring_url_analysis as linkage_analysis

KAFKA_BROKER_URL = os.environ.get('KAFKA_BROKER_URL')
TOPIC_INPUT = os.environ.get('TOPIC_INPUT')
TOPIC_OUTPUT = os.environ.get('TOPIC_OUTPUT')
DATABASE = os.environ.get('DATABASE')

if __name__ == '__main__':
    consumer = kafka.connectConsumer(topic = TOPIC_INPUT, server = KAFKA_BROKER_URL)
    producer = kafka.connectProducer(server = KAFKA_BROKER_URL)
    working = True
    while working:
        dict = kafka.consume(consumer = consumer)
        if(dict != {}):
            for topic, messages in dict.items():
                for message in messages:
                    domain = str(message.value['domain'])
                    print('Received message: '+ domain)
                    clusters = message.value['clusters']
                    labeled_domain = linkage_analysis.calculate_all_cluster_labels(clusters)
                    print('#######Clusters Tagged########')
                    if labeled_domain:
                        content = {
                            'domain': domain,
                            'TaggedClusters': labeled_domain
                        }
                        content_json = json.dumps(content)
                        kafka.send_message(producer=producer, topic=TOPIC_OUTPUT, value=content)
                        try:
                            mongo.put(domain, content_json)
                            print('Data saved on DB')
                        except Exception as ex:
                            print('Failed while saving')
