from zeep import Client
from zeep import xsd
from zeep.plugins import HistoryPlugin

LDB_TOKEN = 'f3b043b1-db1a-41bc-b086-9b4f8fc39df1'
WSDL = 'http://lite.realtime.nationalrail.co.uk/OpenLDBWS/wsdl.aspx?ver=2017-10-01'

if LDB_TOKEN == '':
    raise Exception("Please configure your OpenLDBWS token in getDepartureBoardExample!")

history = HistoryPlugin()

client = Client(wsdl=WSDL, plugins=[history])

header = xsd.Element(
    '{http://thalesgroup.com/RTTI/2013-11-28/Token/types}AccessToken',
    xsd.ComplexType([
        xsd.Element(
            '{http://thalesgroup.com/RTTI/2013-11-28/Token/types}TokenValue',
            xsd.String()),
    ])
)
header_value = header(TokenValue=LDB_TOKEN)

res = client.service.GetDepartureBoard(numRows=10, crs='EUS', _soapheaders=[header_value])

print("Trains at " + res.locationName)
print("===============================================================================")

services = res.trainServices.service

i = 0
while i < len(services):
    t = services[i]
    print(t.std + " to " + t.destination.location[0].locationName + " - " + t.etd)
    #print(t)
    i += 1