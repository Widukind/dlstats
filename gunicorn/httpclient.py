import http.client
import json
#test client
conn = http.client.HTTPConnection("localhost", 8001)
conn.request("GET","/cat_eurostat")
response = conn.getresponse()
print(response.status, response.reason)
data = response.read()
conn.close()
data = data.decode('utf-8')
result = json.loads(data)
