#!/usr/bin/env python3
from http.client import RemoteDisconnected

from zeep import Client
from zeep import xsd
from zeep.plugins import HistoryPlugin
from time import sleep
from kafka import KafkaProducer
from json import dumps
from ast import literal_eval

topic_name = 'train-departures-topic'
servers = ['b-1.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092',
           'b-2.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092',
           'b-3.kafka-enroute.mx95vv.c11.kafka.us-west-2.amazonaws.com:9092']

LDB_TOKEN = 'f3b043b1-db1a-41bc-b086-9b4f8fc39df1'
WSDL = 'http://lite.realtime.nationalrail.co.uk/OpenLDBWS/wsdl.aspx?ver=2017-10-01'

time_interval_in_seconds = 10

crs_list = ['EUS', 'LIV', 'GLC', 'BHM', 'WFJ', 'CRE']
crs_list_size = len(crs_list)

if LDB_TOKEN == '':
    raise Exception("Please configure your OpenLDBWS token in getDepartureBoardExample!")

history = HistoryPlugin()

def initiliaze_client():
    client = Client(wsdl=WSDL, plugins=[history])
    return client


client = initiliaze_client()


header = xsd.Element(
    '{http://thalesgroup.com/RTTI/2013-11-28/Token/types}AccessToken',
    xsd.ComplexType([
        xsd.Element(
            '{http://thalesgroup.com/RTTI/2013-11-28/Token/types}TokenValue',
            xsd.String()),
    ])
)
header_value = header(TokenValue=LDB_TOKEN)


producer = KafkaProducer(bootstrap_servers=servers,
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

print(f"========= Producer process running, Starting sending every {time_interval_in_seconds} seconds messages to {topic_name} ===========\n")



def retrieve_train_departure_data(crs):
    global client
    try:
        res = client.service.GetDepartureBoard(numRows=3, crs=crs, _soapheaders=[header_value])
    except RemoteDisconnected as excpetion:
        print(excpetion)
        print('-------Initialize again the client and retrieving data------')
        client = initiliaze_client()
        res = client.service.GetDepartureBoard(numRows=3, crs=crs, _soapheaders=[header_value])

    print(f"------Trains at {res.locationName} ({crs})------")
    print("===============================================================================")

    services = res.trainServices.service

    i = 0
    departures = []
    while i < len(services):
        t = services[i]
        departures.append(t)
        print(t.std + " to " + t.destination.location[0].locationName + " - " + t.etd)
        i += 1
    return departures


def publish_to_kafka_topic(departures):
    for departure in departures:
        producer.send(topic_name, literal_eval(departure.__str__()))
        producer.flush()


crs_idx = 0
while True:
    departures = retrieve_train_departure_data(crs_list[crs_idx % crs_list_size])
    publish_to_kafka_topic(departures)
    crs_idx += 1
    sleep(time_interval_in_seconds)