from kafka import KafkaProducer
from time import sleep
from json import dumps
from food_model import Food
from jsf import JSF
import boto3
import pytest

@pytest.mark.food
def test_food_pipeline():

    topic_name = 'food-topic'
    servers = ['b-1.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092',
               'b-2.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092',
               'b-3.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092']

    ### Starting Data Injection (Similar as Step 2 of Assignment 4)

    faker = JSF(Food.schema())
    food_input = faker.generate()


    producer = KafkaProducer(bootstrap_servers=servers,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

    print(f"========= Producer process running, Sending message to {topic_name} ===========\n")

    producer.send(topic_name, food_input)
    producer.flush()
    print(f'Injected JSON data sent: {dumps(food_input)}')

    ### Ending Data Ingestion

    sleep(300)

    ### Starting validation phase ((Similar as Step 3 of Assignment 4)

    retries_interval_seconds = 1
    max_retries = 10

    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString=f"select name, scientific_name from sampledb.food_raw where name = '{food_input['name']}'",
        QueryExecutionContext={
            'Database': 'sampledb'
        },
        ResultConfiguration={
            'OutputLocation': 's3://enroute-bucket/athena-logs/enrique-logs/',
        },
        WorkGroup='primary'
    )

    print(response)

    retries = 0
    while True:
        query_execution = client.get_query_execution(
            QueryExecutionId=response['QueryExecutionId']
        )
        print(query_execution)

        execution_state = query_execution['QueryExecution']['Status']['State']
        print(f'Query execution state: {execution_state}')

        if retries == max_retries or execution_state == 'SUCCEEDED':
            break

        retries += 1
        sleep(retries_interval_seconds)


    query_result = client.get_query_results(
        QueryExecutionId=response['QueryExecutionId'],
        MaxResults=10
    )

    print(query_result)

    athena_food_name = query_result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']

    print(f"Food Name from athena table: {athena_food_name}")


    assert food_input['name'] == athena_food_name, f"Injected data {food_input['name']} is not equal to athena table data {athena_food_name}"
