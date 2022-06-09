import requests
import json
import base64
import os

from requests.exceptions import HTTPError
from decouple import config


def encode_to_b64_string(*args):
    string = ""
    for x in args:
        string += x
    string_bytes = string.encode("utf-8")
    string_b64_bytes = base64.b64encode(string_bytes)
    string_b64 = string_b64_bytes.decode("utf-8")
    return string_b64


def run_post_request(url, data, header=None):
    try:
        response = requests.post(url, headers=header, data=data, verify=True)
        response.raise_for_status()
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    else:
        return response


def run_get_request(url, header):
    try:
        response = requests.get(url, headers=header)
        response.raise_for_status()
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
        exit(1)
    except Exception as err:
        print(f'Other error occurred: {err}')
        exit(1)
    else:
        return response


URL_OAUTH_TOKEN = config("URL_OAUTH_TOKEN")
URL_TO_RECORD = config("URL_TO_RECORD")
URL_FILE_UPLOAD = config("URL_FILE_UPLOAD")
URL_GRANT_PERMISSION = config("URL_GRANT_PERMISSION")
URL_CONTENT_DOC_ID = config("URL_CONTENT_DOC_ID")

GRANT_TYPE = config("GRANT_TYPE")
CLIENT_ID = config("CLIENT_ID")
CLIENT_SECRET = config("CLIENT_SECRET")
USERNAME = config("USERNAME")
PASSWORD = config("PASSWORD")

payload = {
    "grant_type": GRANT_TYPE,
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "username": USERNAME,
    "password": PASSWORD
}

oauth_response = run_post_request(URL_OAUTH_TOKEN, data=payload)

oauth_response_json = oauth_response.json()

if oauth_response_json:
    bearer_token = oauth_response_json["access_token"]  # We store the bearer token
    print('Bearer token retrieved.')

    create_record_payload = {
        "Category__c": "Startup",
        "Description__c": "Retrieves Wi-Fi signal quality, quality average, strength and network congestion information \
        as well as engages the user in case of low Wi-Fi signal.",
        "Details_URL__c": "[https://www.nexthink.com/library/chatbot-content-pack/#get-startup-impact]\
        (https://www.nexthink.com/library/chatbot-content-pack/#get-startup-impact)",
        "OS__c": "Windows",
        "Name": "El ultimo"
    }

    create_payload_json = json.dumps(create_record_payload, indent=4)

    headers = {
        "Authorization": "Bearer token" + bearer_token if bearer_token else None,
        "Content-Type": "application/json"
    }

    print(headers)

    creation_response = run_post_request(URL_TO_RECORD, header=headers, data=create_payload_json)

    print(creation_response)

    if creation_response:
        creation_response_json = creation_response.json()

    if creation_response_json["success"]:

        file = ("/Users/tswar/source/scriptstosign/RemoteActions/RemoteWorkers/"
                "Get-WiFiSignalStrengthNexthinkIT/Get-WiFiSignalStrengthNexthinkIT.ps1")

        with open(file, encoding='utf-8') as f:
            b64_file = encode_to_b64_string(f.read())

        file_upload_payload = {
            'VersionData': b64_file,
            "Title": "Get-WiFiSignalStrengthNexthinkIT.ps1",
            "PathOnClient": "Get-WiFiSignalStrengthNexthinkIT.ps1",
            #"FirstPublishLocationId": creation_response_json["id"],
            #"SharingOption": "A",
            #"SharingPrivacy": "N"
        }

        file_upload_json = json.dumps(file_upload_payload, indent=4)

        fileupload_response = run_post_request(URL_FILE_UPLOAD, header=headers, data=file_upload_json)

        print(fileupload_response.text)



        if fileupload_response:
            fileupload_response_json = fileupload_response.json()

            print(fileupload_response_json)

            if fileupload_response_json["success"]:

                filled_url = URL_CONTENT_DOC_ID.format(fileupload_response_json["id"])

                print(filled_url)

                content_id_response = run_get_request(filled_url, header=headers)

                content_id_json = content_id_response.json()

                print(content_id_json["records"][0]["ContentDocumentId"])

                content_id = content_id_json["records"][0]["ContentDocumentId"]

                file_perm_payload = {
                    "ContentDocumentId": content_id,
                    "ShareType": "V",
                    "Visibility": "AllUsers",
                    "LinkedEntityId": creation_response_json["id"]
                }

                # file_perm_payload = {
                #     "Name": "Get-WiFiSignalStrengthNexthinkIT.ps1",
                #     "ContentVersionId": fileupload_response_json["id"],
                #     "PreferencesAllowViewInBrowser": True
                # }
                file_perm_json = json.dumps(file_perm_payload, indent=4)

                file_perm_response = run_post_request(URL_GRANT_PERMISSION, header=headers, data=file_perm_json)

                print(file_perm_response.json())


else:
    print("Cannot get Bearer token")
    exit(1)



