from kafka import KafkaConsumer

topic_name = 'train-departures-topic'
servers = ['b-1.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092',
           'b-2.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092',
           'b-3.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092']

consumer = KafkaConsumer(topic_name, bootstrap_servers=servers)

print(f"========= Consumer process running, reading messages from {topic_name} ===========\n")

for message in consumer:
    print(message)