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


module_logger = logging.getLogger('waitress.salesforce')


class Salesforce:
    """A Salesforce management class for API

    This class provides utilities to manage Salesforce content for the RA Library. It uses CRUD mechanism for
    records.

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
    URL_DELETE_ONE = config("URL_DELETE_ONE")
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
        self.logger = logging.getLogger("waitress.salesforce.Salesforce")
        self.logger.debug("Initiating Salesforce API object")
        self._bearer_token = None
        self._header = None

        self._get_bearer_token()  # We update the self._bearer_token with a new token
        self._create_header()  # We update the self._header with token and content type
        self._existing_records = self._get_all_records()  # get all existing records in Salesforce

    @staticmethod
    def encode_to_b64_string(string):
        """Generate a base64 string

        Utility function to encode string to base64 for URL support.

        :param string: a string
        :type string: str
        :return: the name of the currently active user
        :rtype: str
        """
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
        self._header = header
        self.logger.debug("Header updated with Bearer token and Content-Type")

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
            self.logger.error("Cannot retrieve token. Waitress will exit.")
            exit(1)
        except Exception as err:
            self.logger.exception(err)
            self.logger.error("Cannot retrieve token. Waitress will exit.")
            exit(1)
        else:
            oauth_response_json = oauth_response.json()
            bearer_token = oauth_response_json["access_token"]  # We store the bearer token
            self.logger.debug("Bearer token retrieved successfully.")
            self._bearer_token = bearer_token

    def _run_http_request(self, request_type, url, payload=None):
        """
        Method that runs an HTTP request using requests package

        :param request_type: The type of request e.g. POST, GET, DELETE
        :param url: the endpoint URL
        :param payload: the payload if any
        :return: An http response

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
        """Get all the existing RA records from Salesforce.

        This method uses this process:
            * Query all the existing records via GET and retrieve a JSON with all object and their ID
            * If the records DB is not empty, get the record ID for all the records
            * For each record, create a URL with the ID and query the details for the record
            * Store the results in a dataframe

        :return: a pandas dataframe with all RA records, otherwise an empty dataframe
        :rtype: pd.DataFrame

        """
        all_records_response = self._run_http_request("GET", self.URL_QUERY_ALL)  # Query all existing records
        if all_records_response:  # if request is successful
            if all_records_response.json()["totalSize"] > 0:  # if there are records
                all_records_json = all_records_response.json()  # we create a JSON from the response
                records = all_records_json["records"]  # Extract records information
                list_records = []
                for record in records:  # Loop over the existing records
                    record_url = self.URL_TO_RECORD + record["Id"]  # Create a URL to query a record for a specific ID
                    response_record = self._run_http_request("GET", record_url)  # Query data for a specific record
                    if not response_record:  # If we cannot get the record data, go to next record
                        self.logger.error(f"Cannot get record for {record['ID']}")
                        continue
                    record_json = response_record.json()  # if successful, we parse the response
                    local_dict = {  # Create a dictionary with returned data
                        "Id": record_json.get("Id"),
                        "Name": record_json.get("Name"),
                        "LastModifiedDate": record_json.get("LastModifiedDate"),
                        "Description": record_json.get("Description__c"),
                        "Category": record_json.get("Category__c"),
                        "OS": record_json.get("OS__c"),
                        "Details": record_json.get("Details_URL__c")
                    }
                    list_records.append(local_dict)  # Append the record to the list
                return pd.DataFrame(list_records)  # Return the created df from the list
            else:
                self.logger.info("No records found. RA Library is empty.")
                return pd.DataFrame()  # We return an empty dataframe
        else:
            self.logger.error("Couldn't query the Salesforce API to get the full records list. Program will close.")
            exit(1)

    @property
    def existing_records(self):
        if not self._existing_records.empty:
            return self._existing_records
        return pd.DataFrame()

    def _create_ra_record(self, df_row):
        """ Create a RA record in the database

        Create a record in Salesforce database by creating a JSON payload, providing:
            * Category__c (must be pre-created in Salesforce)
            * Description__c (free text to describe the RA)
            * Details_URL__c (link to the V6 Library documentation)
            * OS__c (support OS, must be pre-created in Salesforce)
            * Name (name of the RA, free text)
        :param df_row: a dataframe row
        :type df_row: pd.Series
        :return: a response object if success, None otherwise
        :rtype: requests.Response or None
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
        except KeyError as keyErr:
            self.logger.exception(keyErr)
            return
        except Exception as err:
            self.logger.exception(err)
            return
        else:
            create_record_json = json.dumps(create_record_dict, indent=4)
            create_record_response = self._run_http_request("POST", self.URL_TO_RECORD, payload=create_record_json)
            return create_record_response

    def _upload_json_file(self, df_row):
        """ Create a RA record in the database

        Create a record in Salesforce database by creating a JSON payload, providing:
            * Category__c (must be pre-created in Salesforce)
            * Description__c (free text to describe the RA)
            * Details_URL__c (link to the V6 Library documentation)
            * OS__c (support OS, must be pre-created in Salesforce)
            * Name (name of the RA, free text)
        :param df_row: a dataframe row
        :type df_row: pd.Series
        :return: a response object if success, None otherwise
        :rtype: requests.Response or None
        """
        try:
            self.logger.debug(f"Uploading JSON file {df_row['Path']} for {df_row['Name']}")
            json_file_path = df_row["Path"]
            with open(json_file_path, encoding='utf-8') as f:
                base64_file = self.encode_to_b64_string(f.read())  # TODO Implement try catch
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
        except AttributeError as attrErr:
            self.logger.exception(attrErr)
            return
        except KeyError as keyErr:
            self.logger.exception(keyErr)
            return
        except Exception as err:
            self.logger.exception(err)
            return

        if not content_id_response:
            return

        try:
            content_id_json = content_id_response.json()
            content_id = content_id_json["records"][0]["ContentDocumentId"]
            file_perm_dict = {
                "ContentDocumentId": content_id,
                "ShareType": "V",
                "Visibility": "AllUsers",
                "LinkedEntityId": create_record_json["id"]
            }
        except KeyError as keyErr:
            self.logger.exception(keyErr)
            return
        except AttributeError as attrErr:
            self.logger.exception(attrErr)
            return
        except Exception as err:
            self.logger.exception(err)
            return
        else:
            file_perm_json = json.dumps(file_perm_dict, indent=4)
            file_perm_response = self._run_http_request("POST", self.URL_GRANT_PERMISSION, payload=file_perm_json)
            return file_perm_response

    def delete_one_ra(self, record_id: str) -> bool:
        """ Delete a RA record in Salesforce

        Runs an HTTP DELETE request on the endpoint to delete a specific record
        :param record_id: the ID of the RA record
        :type record_id: str
        :return: True if the deletion was a success, False otherwise
        :rtype: bool

        """
        self.logger.debug(f"Deleting RA with ID {record_id}")
        url_delete = self.URL_DELETE_ONE.format(record_id)
        delete_response = self._run_http_request("DELETE", url_delete, None)
        try:
            delete_json = delete_response.json()
        except Exception as err:
            self.logger.exception(err)
            return False
        else:
            if delete_json["success"]:
                self.logger.debug(f"Record ID : {record_id} successfully deleted")
                return True
            else:
                return False

    def delete_all_ras(self):
        """ Delete all the records in salesforce

        Method that deletes all the records in Salesforce. It does the following:
            * Check if self.existing_records is not empty (i.e. RA DB is not empty)
            * Create a string with all concatenated RA Salesforce IDs
            * Create one URL with all IDs to delete the RA DB (allOrNone=false meaning won't fail if one delete fails)
            * Perform a full delete.
            * If some records fail being deleted write them in the log
            * If all items are deleted return True
        :return: True if delete successful False otherwise
        :rtype: bool

        """
        df = self.existing_records
        if not df.empty:
            self.logger.info(f"There are {len(df.index)} RA records in the RA Library.")
            record_ids_list = df.loc[:, "Id"].to_list()
            records_str = ",".join(record_ids_list)
            delete_all_url = self.URL_DELETE_ALL + records_str + "&allOrNone=false"
            delete_response = self._run_http_request("DELETE", delete_all_url, None)
            if not delete_response:
                self.logger.error("Couldn't delete records in Salesforce. Check the endpoint URL or API access.")
                return False
            delete_json = delete_response.json()
            failed_deletion_ids = [item["id"] for item in delete_json if not item["success"]]
            if failed_deletion_ids:
                self.logger.error(f"Failed to delete the records with IDs  {failed_deletion_ids['id']}")
            if len(failed_deletion_ids) == len(df.index):
                self.logger.info("All records were deleted from the RA Library")
            return True
        else:
            self.logger.info("No records to delete.")
            return True

    def process_dataframe(self, df, from_scratch):
        """ Process the dataframe provide, creating records in Salesforce

        The process works in the following way.
            * from_scratch parameters deletes all RA records of the Salesforce DB. The option is provided by the user
            * Before creating a record we check if it already exists. If it does, we delete it and recreate it as per
            the newer version provided
        :param df:
        :param from_scratch:
        :param replace_existing:
        :return:
        """
        if from_scratch:
            delete_status = self.delete_all_ras()
            if not delete_status:
                self.logger.error("Unable to create the RA database from scratch. Waitress will exit.")
                exit(1)
            self._existing_records = pd.DataFrame()

        for index, row in df.iterrows():
            if not self.existing_records.empty:
                if row["Name"] in self.existing_records["Name"].values:
                    id_record_to_delete = self.existing_records. \
                            loc[self.existing_records["Name"] == row["Name"], "Id"].values[0]  # TODO handle various ids
                    self.logger.debug(f"{row['Name']} already exists in the library. It will be replaced.")
                    self.logger.debug(f"Deleting record for RA Name : {row['Name']}")
                    deletion_status = self.delete_one_ra(id_record_to_delete)
                    if not deletion_status:  # If we cannot delete the record, we go to next record
                        self.logger.error(f"Couldn't delete record for {row['Name']}")
                        continue
            # RA record creation
            self.logger.debug(f"Creating record for RA Name : {row['Name']}")
            create_record_response = self._create_ra_record(row)
            if not create_record_response:  # If no response exists
                self.logger.error(f"Cannot reach endpoint to create record for RA Name : {row['Name']}")
                continue

            if not create_record_response.json()["success"]:  # If creation not successful
                self.logger.error(f"Cannot create record for RA Name : {row['Name']}")
                continue
            self.logger.debug(f"Record created with success for RA Name : {row['Name']}")

            # RA file upload
            self.logger.debug(f"Uploading JSON file for RA Name : {row['Name']}")
            file_upload_response = self._upload_json_file(row)
            if not file_upload_response:
                self.logger.error(f"Cannot reach endpoint to upload JSON file for RA Name : {row['Name']}")
                continue
            if not file_upload_response.json()["success"]:  # If file NOT upload successfully
                self.logger.error(f"Cannot upload file for RA Name : {row['Name']}")
                continue
            self.logger.debug(f"RA JSON file successfully upload for RA Name : {row['Name']}")

            # Granting record permissions
            self.logger.debug(f"Granting permission for record for RA Name : {row['Name']}")
            file_perm_response = self._grant_permission(create_record_response, file_upload_response)
            if not file_perm_response:
                self.logger.error(f"Cannot upload file for RA Name : {row['Name']}")
                continue
            if not file_perm_response.json()["success"]:  # If permission NOT given successfully
                continue
            self.logger.info(f"{row['Name']} was loaded to Salesforce successfully.")
