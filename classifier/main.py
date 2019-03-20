from kafka import KafkaConsumer


print('Running Consumer')
topic_name='DominiTrovati'
consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
for msg in consumer:
    print('message received')