from kafka import KafkaConsumer


def connect(topic, server):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=server)
    return consumer

def consume(consumer):
    # Consume messages
    print("###############READ################")
    for message in consumer:
        print("Received message: "+str(message))

