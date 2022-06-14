import boto3
from time import sleep

client = boto3.client('athena')
response = client.start_query_execution(
    QueryString="select name, scientific_name from sampledb.food_raw where name = 'veniam Lorem'",
    QueryExecutionContext={
        'Database': 'sampledb'
    },
    ResultConfiguration={
        'OutputLocation': 's3://enroute-bucket/athena-logs/enrique-logs/',
    },
    WorkGroup='primary'
)

print(response)

sleep(15)


query_result = client.get_query_results(
    QueryExecutionId=response['QueryExecutionId'],
    MaxResults=10
)

print(query_result)