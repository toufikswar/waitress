import requests
from decouple import config
import pandas as pd

#URL = config("RA_RECORD_URL")

QUERY_URL = "https://nexthink--luis.my.salesforce.com/services/data/v53.0/query/?q=SELECT+Id+from+Remote_Action__c"

payload = {}
headers = {
  'Authorization': 'Bearer 00D5t0000004f0D!ARoAQF6EzYIIhc0ZjOPRoVRP4EQa2CMAVQ1MSAKjbQHPmsz0TqgtgYHm_oNgqPwt.M3OSHcE5TMKrCGh2BHTZAmgEIVfWGOu'
}

response = requests.request("GET", QUERY_URL, headers=headers, data=payload)

response_json = response.json()

records = response_json["records"]

print(response_json)

list_records = []
for record in records:
    record_url = URL + record["Id"]
    response_record = requests.request("GET", record_url, headers=headers, data=payload)
    response_record_json = response_record.json()
    print(response_record_json)
    local_dict = {
        "Id": response_record_json["Id"],
        "Name": response_record_json["Name"],
        "LastModifiedDate": response_record_json["LastModifiedDate"],
        "Description": response_record_json["Description__c"],
        "Category": response_record_json["Category__c"],
        "OS": response_record_json["OS__c"],
        "Details": response_record_json["Details_URL__c"]
    }
    list_records.append(local_dict)

df_existing = pd.DataFrame(list_records)

print(df_existing)
