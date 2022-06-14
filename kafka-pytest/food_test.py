from kafka import KafkaProducer
from time import sleep
from json import dumps
from food_model import Food
from jsf import JSF

topic_name = 'food-topic'
servers = ['b-1.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092',
           'b-2.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092',
           'b-3.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092']



faker = JSF(Food.schema())

food_input = faker.generate()
print(food_input)
print(dumps(food_input))

# print(isinstance(food_input, dict)) # True (is a dict)
# print(type(food_input)) # <class 'dict'>


producer = KafkaProducer(bootstrap_servers=servers,
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

print(f"========= Producer process running, Starting sending every 20 seconds messages to {topic_name} ===========\n")
counter = 1

while True:
    producer.send(topic_name, food_input)
    producer.flush()
    print(f'json sent!')
    counter = counter + 1
    sleep(20)