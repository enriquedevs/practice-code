from kafka import KafkaProducer
from time import sleep
from json import dumps

topic_name = 'food-topic'
servers = ['b-1.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092',
           'b-2.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092',
           'b-3.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092']

producer = KafkaProducer(bootstrap_servers=servers,
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

json_string = """
{"name":"Angelica","scientific_name":"Angelica keiskei","group_name": {"name": "Herbs and Spices", "sub_group":[{"name":"Herbs"}, {"name":"Spices"}]}}
"""


print(f"========= Producer process running, Starting sending every 20 seconds messages to {topic_name} ===========\n")
counter = 1

while True:
    producer.send(topic_name, json_string)
    producer.flush()
    print(f'json sent!')
    counter = counter + 1
    sleep(20)