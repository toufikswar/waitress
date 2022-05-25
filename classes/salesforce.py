import pandas
import pandas as pd
import requests
import json
import base64
import os
import logging

from typing import List
from decouple import config
from requests.exceptions import HTTPError


module_logger = logging.getLogger('fuser.salesforce')


class Salesforce:
    """ A Salesforce management class

    This class provides utilities to manage Salesforce content for the RA Library

    Attribute:
        - URL_OAUTH_TOKEN(str): the url to get a bearer token
        - URL_QUERY_ALL(str): the url to query all records
        - URL_TO_RECORD(str): the url to query one record
        - URL_FILE_UPLOAD(str): the url to upload a file
        - URL_CONTENT_DOC_ID(str): the url to get the Content Document ID
        - URL_GRANT_PERMISSION(str): the url to grant the file public permission
    """
    # Class attributes for Authentication and URLs
    URL_OAUTH_TOKEN = config("URL_OAUTH_TOKEN")
    URL_QUERY_ALL = config("URL_QUERY_ALL")
    URL_DELETE_ALL = config("URL_DELETE_ALL")
    URL_TO_RECORD = config("URL_TO_RECORD")
    URL_FILE_UPLOAD = config("URL_FILE_UPLOAD")
    URL_CONTENT_DOC_ID = config("URL_CONTENT_DOC_ID")
    URL_GRANT_PERMISSION = config("URL_GRANT_PERMISSION")
    #  Class attributes for credentials
    GRANT_TYPE = config("GRANT_TYPE")
    CLIENT_ID = config("CLIENT_ID")
    CLIENT_SECRET = config("CLIENT_SECRET")
    USERNAME = config("USERNAME")
    PASSWORD = config("PASSWORD")

    def __init__(self):
        """

        """
        self.logger = logging.getLogger("fuser.salesforce.Salesforce")
        self.logger.debug("Initiating Salesforce API object")
        self._bearer_token = self._get_bearer_token()
        self._header = self._create_header()
        self._existing_records = self._get_all_records()

    @staticmethod
    def encode_to_b64_string(*str_list: List[str]) -> str:
        """

        :param str_list:
        :return: the name of the currently active user
        :rtype: str
        """
        string = ""
        for x in str_list:
            string += x
        string_bytes = string.encode("utf-8")
        string_b64_bytes = base64.b64encode(string_bytes)
        string_b64 = string_b64_bytes.decode("utf-8")
        return string_b64

    def _create_header(self):
        """ Create the HTTP header
        Method that inserts the bearer token in the header and return the header
        :return: dict
        """
        header = {"Authorization": "Bearer token" + self._bearer_token,
                  "Content-Type": "application/json"}
        self.logger.debug("Header updated with Bearer token and Content-Type")
        return header

    def _get_bearer_token(self):
        """ Get the Bearer token from Salesforce instance
        :return: str
        """
        payload = {"grant_type": self.GRANT_TYPE, "client_id": self.CLIENT_ID, "client_secret": self.CLIENT_SECRET,
                   "username": self.USERNAME, "password": self.PASSWORD}
        try:
            oauth_response = requests.post(self.URL_OAUTH_TOKEN, data=payload)
            oauth_response.raise_for_status()
        except HTTPError as http_err:
            self.logger.exception(http_err)
            self.logger.error("Cannot retrieve token. Program will exit.")
            raise SystemExit
        except Exception as err:
            self.logger.exception(err)
            self.logger.error("Cannot retrieve token. Program will exit.")
        else:
            oauth_response_json = oauth_response.json()
            bearer_token = oauth_response_json["access_token"]  # We store the bearer token
            self.logger.debug("Bearer token retrieved.")
            return bearer_token

    def _run_http_request(self, request_type, url, payload=None):
        """

        :param type:
        :param url:
        :param payload:
        :return:
        """
        try:
            response = requests.request(request_type, url, headers=self._header, data=payload)
            response.raise_for_status()
        except HTTPError as http_err:
            self.logger.exception(http_err)
        except Exception as err:
            self.logger.exception(err)
        else:
            self.logger.debug(f"{request_type} request successfully executed.")
            return response

    def _run_post_request(self, url, payload):
        """ Run an HTTP POST request
        :param url: str
        :param payload: dict
        :return: response object
        """
        try:
            response = requests.post(url, headers=self._header, data=payload)
            response.raise_for_status()
        except HTTPError as http_err:
            self.logger.exception(http_err)
        except Exception as err:
            self.logger.exception(err)
        else:
            self.logger.debug("POST request successfully executed.")
            return response

    def _run_get_request(self, url):
        """ Run an HTTP GET request
        :param url: str
        :return: a response object
        """
        try:
            response = requests.get(url, headers=self._header)
            response.raise_for_status()
        except HTTPError as http_err:
            self.logger.exception(http_err)
        except Exception as err:
            self.logger.exception(err)
        else:
            return response

    def _get_all_records(self):
        """ Get all the existing RA records from Salesforce
        :return: a pandas dataframe
        """
        all_records_response = self._run_http_request("GET", self.URL_QUERY_ALL)
        if all_records_response and all_records_response.json()["totalSize"] > 0:
            all_records_json = all_records_response.json()
            records = all_records_json["records"]
            list_records = []
            for record in records:  # Loop over the existing records
                record_url = self.URL_TO_RECORD + record["Id"]
                #  response_record = requests.request("GET", record_url, headers=self._header)
                response_record = self._run_http_request("GET", record_url)
                record_json = response_record.json()
                local_dict = {
                    "Id": record_json.get("Id"),
                    "Name": record_json.get("Name"),
                    "LastModifiedDate": record_json.get("LastModifiedDate"),
                    "Description": record_json.get("Description__c"),
                    "Category": record_json.get("Category__c"),
                    "OS": record_json.get("OS__c"),
                    "Details": record_json.get("Details_URL__c")
                }
                list_records.append(local_dict)
            return pd.DataFrame(list_records)
        else:
            self.logger.info("No records found. RA Library is empty")
            return pd.DataFrame()

    @property
    def existing_records(self):
        if isinstance(self._existing_records, pd.DataFrame) and not self._existing_records.empty:
            return self._existing_records
        return pd.DataFrame()

    def _create_ra_record(self, df_row):
        """ Create a RA record
        :param df_row: a dataframe row
        :return: a response object
        """
        self.logger.debug("Creating record for " + df_row["Name"])
        try:
            create_record_dict = {
                "Category__c": df_row["Category"],
                "Description__c": df_row["Description"],
                "Details_URL__c": df_row["Doc"],  # TODO Create a field for RA versions in SF
                "OS__c": df_row["Type"],
                "Name": df_row["Name"]
            }
            create_record_json = json.dumps(create_record_dict)
        except KeyError as keyErr:
            self.logger.exception(keyErr)
        except Exception as err:
            self.logger.exception(err)
        else:
            create_record_json = json.dumps(create_record_dict, indent=4)
            create_record_response = self._run_http_request("POST", self.URL_TO_RECORD, payload=create_record_json)
            return create_record_response

    def _upload_json_file(self, df_row):
        """ Upload a JSON RA file to Salesforce

        Parameters
        ----------
            df_row
                a pandas.Dataframe()

        Returns
        -------
            file_upload_response
                A requests.Response(), None otherwise
        """
        try:
            self.logger.debug(f"Uploading file {df_row['Path']}")
            json_file_path = df_row["Path"]
            with open(json_file_path, encoding='utf-8') as f:
                base64_file = self.encode_to_b64_string(f.read())
            file_upload_dict = {
                'VersionData': base64_file,
                "Title": os.path.basename(json_file_path),
                "PathOnClient": os.path.basename(json_file_path),
            }
        except FileNotFoundError as fileErr:
            self.logger.exception(fileErr)
            return
        except KeyError as keyErr:
            self.logger.exception(keyErr)
            return
        except Exception as err:
            self.logger.exception(err)
            return
        else:
            file_upload_json = json.dumps(file_upload_dict, indent=4)
            file_upload_response = self._run_http_request("POST", self.URL_FILE_UPLOAD, payload=file_upload_json)
            return file_upload_response

    def _grant_permission(self, create_record_response, file_upload_response):
        """Grant the public permission to the upload JSON file.

        Parameters
        ----------
        create_record_response
            Human readable string describing the exception.
        file_upload_response
            Numeric error code.

        Returns
        -------
        object
            requests.Response(), otherwise None
        """
        try:
            self.logger.debug(f"Granting file the view permissions")
            create_record_json = create_record_response.json()
            file_upload_json = file_upload_response.json()
            file_perm_url = self.URL_CONTENT_DOC_ID.format(file_upload_json["id"])
            content_id_response = self._run_http_request("GET", file_perm_url)
            content_id_json = content_id_response.json()
        except KeyError as keyErr:
            self.logger.exception(keyErr)
        except Exception as err:
            self.logger.exception(err)
        else:
            if content_id_response:
                try:
                    content_id = content_id_json["records"][0]["ContentDocumentId"]
                    file_perm_dict = {
                        "ContentDocumentId": content_id,
                        "ShareType": "V",
                        "Visibility": "AllUsers",
                        "LinkedEntityId": create_record_json["id"]
                    }
                except KeyError as keyErr:
                    self.logger.exception(keyErr)
                except Exception as err:
                    self.logger.exception(err)
                else:
                    file_perm_json = json.dumps(file_perm_dict, indent=4)
                    file_perm_response = self._run_http_request("POST", self.URL_GRANT_PERMISSION, payload=file_perm_json)
                    return file_perm_response
        return

    def process_dataframe(self, df):
        """

        Returns
        -------
        object
            rererere
        """
        for index, row in df.iterrows():
            create_record_response = self._create_ra_record(row)
            if create_record_response and create_record_response.json()["success"]:  # If creation successful
                self.logger.debug(f"Record created with success for {row['Name']}")
                file_upload_response = self._upload_json_file(row)
                if file_upload_response and file_upload_response.json()["success"]:  # If file upload successful
                    self.logger.debug(f"RA file successfully upload for  {row['Name']}")
                    file_perm_response = self._grant_permission(create_record_response, file_upload_response)
                    if file_perm_response and file_perm_response.json()["success"]: # If permission given successfully
                        self.logger.info(f"{row['Name']} was loaded to Salesforce successfully.")
                    else:
                        self.logger.info(f"Cannot grant permissions to file for {row['Name']}.")
                        continue  # Move on to next row (record)
                else:
                    self.logger.info(f"Cannot upload file for {row['Name']}.")
                    continue  # Move on to next row (record)
            else:
                self.logger.info(f"Cannot create record for {row['Name']}.")
                continue  # Move on to next row (record)

    def delete_all_ras(self):
        """

        :return:
        """
        if isinstance(self.existing_records, pd.DataFrame):
            df = self.existing_records
            if not df.empty:
                self.logger.info(f"There are {df.__len__} RA records in the RA Library.")
                record_ids_list = df.loc[:, "Id"].to_list()
                records_str = ",".join(record_ids_list)
                self.URL_DELETE_ALL = self.URL_DELETE_ALL + records_str + "&allOrNone=false"
                delete_response = self._run_http_request("DELETE", self.URL_DELETE_ALL, None)
                print(delete_response.json())
                delete_json = delete_response.json()
                failed_deletion_ids = [item["id"] for item in delete_json if not item["success"]]
                if failed_deletion_ids:
                    self.logger.error(f"Failed to delete these items {failed_deletion_ids['id']}")
                if len(failed_deletion_ids) == df.__len__():
                    self.logger.info("All records were deleted from RA Library")
            else:
                self.logger.info("No records to delete.")




