import requests
from decouple import config
import pandas as pd

URL = config("URL_TO_RECORD")

QUERY_URL = "https://nexthink--luis.my.salesforce.com/services/data/v53.0/query/?q=SELECT+Id+from+Remote_Action__c"

DELETE_URL = "https://nexthink--luis.my.salesforce.com/services/data/v54.0/composite/sobjects?ids="

payload = {}
headers = {
  'Authorization': 'Bearer 00D5t0000004f0D!ARoAQF6EzYIIhc0ZjOPRoVRP4EQa2CMAVQ1MSAKjbQHPmsz0TqgtgYHm_oNgqPwt.M3OSHcE5TMKrCGh2BHTZAmgEIVfWGOu'
}

response = requests.request("GET", QUERY_URL, headers=headers, data=payload)

response_json = response.json()

records = response_json["records"]

record_ids_list = [record["Id"] for record in records]

records_str = ",".join(record_ids_list)

DELETE_URL = DELETE_URL + records_str + "&allOrNone=false"

delete_response = requests.request("DELETE", DELETE_URL,headers=headers)

print(delete_response.text)