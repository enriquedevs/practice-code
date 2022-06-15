import boto3
from time import sleep

retries_interval_seconds = 1
max_retries = 10

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

retries = 0
while True:
    query_execution = client.get_query_execution(
        QueryExecutionId=response['QueryExecutionId']
    )
    print(query_execution)

    execution_status = query_execution['QueryExecution']['Status']['State']
    print(f'Query execution state: {execution_status}')

    if retries == max_retries or execution_status == 'SUCCEEDED':
        break

    retries += 1
    sleep(retries_interval_seconds)


query_result = client.get_query_results(
    QueryExecutionId=response['QueryExecutionId'],
    MaxResults=10
)

print(query_result)