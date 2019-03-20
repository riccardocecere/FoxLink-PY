import json
import web_discovery_searx as searx
import kafka_interface as kafka

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
    message_producer=kafka.connect()
    topic_name="DominiTrovati"
    i=1
    print(sites_set)
    for elem in sites_set:
        try:
            kafka.send_message(message_producer,topic_name,i,str(elem))
            print("message sent: "+str(i)+"-"+str(elem))
        except Exception as ex:
            print(ex)
        i+=1
    print('Stop producer')

if __name__ == "__main__":
    main()