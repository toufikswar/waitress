import os
import json
import logging
import pandas as pd

from datetime import datetime
from decouple import config

module_logger = logging.getLogger('waitress.parser')


class JsonParser:

    def __init__(self, config_file):
        self.logger = logging.getLogger("waitress.parser.JsonParser")  # Initialize the logger object
        self._load_config(config_file)  # Load configuration

        self.logger.info(f"Initiating the JSON Parser Object - Will parse JSON files in {self._path_to_json}")
        self._df_category = self._load_data_categories()  # Load RAs categories from Excel file

        self._df_json = pd.DataFrame()  # Df to store data from JSON files
        self._df_all = pd.DataFrame()  # Df to merge JSON file data with Category Excel file


    def _load_config(self, config_file):
        self.logger.info(f"Instantiating Parser Object with {config_file['env']} configuration")
        self._path_to_json = config_file["path_to_json"]
        self._remote_actions_metadata = config_file["remote_actions_metadata"]

    def _load_data_categories(self):
        """
        Loads the categories from prefilled Excel file.

        :return: a dataframe
        """
        try:
            self.logger.info(f"Loading RA metadata from {self._remote_actions_metadata}")
            df_category = pd.read_excel(self._remote_actions_metadata, index_col=0)
        except FileNotFoundError as fileErr:
            self.logger.exception(fileErr)
            self.logger.error("Couldn't load categories data - check path to Excel file. Waitress will close.")
            exit(1)
        except PermissionError as permErr:
            self.logger.exception(permErr)
            self.logger.error("Couldn't load categories data - check path to Excel file. Waitress will close.")
            exit(1)
        except Exception as err:
            self.logger.exception(err)
            self.logger.error("Couldn't load categories data - check path to Excel file. Waitress will close.")
            exit(1)
        else:
            self.logger.info(f"Categories successfully loaded from : {self._remote_actions_metadata}")
            return df_category

    def parse_json_folder(self):
        """
        Parse the JSON folder specified in prod.env, retrieve RA metadata info and store it in self._df_json

        :return: None
        """
        try:
            if not os.path.exists(self._path_to_json):
                self.logger.error(f"JSON Folder {self._path_to_json} do not exists. Make sure it does.")
                exit(1)
            ra_list = []
            for root, dirs, files in os.walk(self._path_to_json):
                for filename in files:
                    file_fullpath = os.path.join(root, filename)
                    if filename.endswith(".json"):
                        returned_dict = self._read_json_file(file_fullpath)
                        if returned_dict:
                            ra_list.append(returned_dict)
                        else:
                            self.logger.error(f"Couldn't read JSON file {file_fullpath} and extract metadata")
        except FileNotFoundError as fileErr:
            self.logger.exception(fileErr)
            self.logger.error("Couldn't parse JSON folder. Check if the folder exists. Waitress will close.")
            exit(1)
        except PermissionError as permErr:
            self.logger.exception(permErr)
            self.logger.error("Couldn't parse JSON folder. Check if the folder permissions. Waitress will close.")
            exit(1)
        except Exception as err:
            self.logger.exception(err)
            self.logger.error("Couldn't parse JSON folder. Waitress will close.")
            exit(1)
        else:
            if ra_list:
                self._df_json = pd.DataFrame(ra_list)
            else:
                self.logger.warning("No JSON files found in the specified location. Waitress will exit.")
                exit(0)

    @property
    def df_json(self):
        """
        Returns the df with RA JSON or an empty dataframe

        :return: self._df_json or an empty df

        """
        return self._df_json

    @property
    def df_category(self):
        """
        Returns the df with RA categories or an empty dataframe

        :return: self._df_category or an empty df
        """
        return self._df_category

    @property
    def df_all(self):
        if self.df_category.empty or self.df_json.empty:
            logging.error("Cannot retrieve full RA dataframe. Check if RA metadata coming from JSON files "
                          "and from Excel Category file are not empty. Waitress will exit.")
            exit(0)
        else:
            try:
                self._df_all = pd.merge(self.df_category, self.df_json, on="Name", how="left")
            except KeyError as keyErr:
                self.logger.exception(keyErr)
                self.logger.error("Couldn't get the full RA metadata. Check the column names match for the merge. "
                                  "Waitress will close.")
                exit(1)
            except Exception as ex:
                self.logger.exception(ex)
                self.logger.error("Couldn't get the full RA metadata. Waitress will close.")
                exit(1)
            else:
                self.logger.info("Merge between Metadata and JSON data successful.")
                return self._df_all

    def _read_json_file(self, file_path):
        """
        Open and read RA metadata from a JSON file.

        :param file_path: the path of the JSON RA
        :type file_path : str
        :return: a dict or None
        """
        if os.path.exists(file_path):
            try:
                with open(file_path) as json_file:  # Open the JSON file and extract the data
                    data_json = json.load(json_file)
                    local_dict = {  # We use get() so if value doesn't exist None is returned
                        "Name": data_json.get("name"),
                        "Description": data_json.get("description"),
                        "Purpose": data_json.get("purpose"),
                        "Type": self._get_ra_type(data_json),
                        "Path": file_path
                    }
            except FileNotFoundError as fileErr:
                self.logger.exception(fileErr)
                return
            except PermissionError as permErr:
                self.logger.exception(permErr)
                return
            except Exception as err:
                self.logger.exception(err)
                return
            else:
                self.logger.debug(f"{data_json['name']} JSON file parsed with success.")
                return local_dict
        else:
            self.logger.error(f"{file_path} doesn't exist.")
            return

    def _get_ra_type(self, json_obj):
        """
        Read the JSON fields and assigned a type to the RA i.e. Windows, macOS, Combined or ""

        :param json_obj:
        :return: str
        """
        script_info = json_obj.get("scriptInfo")
        if not script_info:
            self.logger.error("No scriptInfo field found in the JSON file")
            return ""
        if script_info.get("scriptWindows") and script_info.get("scriptMacOs"):
            return "Combined"
        elif script_info.get("scriptWindows") and script_info.get("scriptMacOs") is None:
            return "Windows"
        elif script_info.get("scriptWindows") is None and script_info.get("scriptMacOs"):
            return "macOS"
        else:
            self.logger.error(f"No info found on the OS compatible for {json_obj.get('name')}")
            return ""

    def save_to_excel(self, df, name):
        """
        Save the df to an Excel file.

        :param df: a pandas.DataFrame
        :param name: str the name of the Excel file
        :return: None
        """
        now = datetime.now()
        date_now = now.strftime("%m-%d-%Y-%H-%M-%S")
        excel_filename = f"./data/{name}_{date_now}.xlsx"
        with pd.ExcelWriter(excel_filename) as writer:
            try:
                df.to_excel(writer, sheet_name="all_remote_actions")
            except IndexError as indErr:
                self.logger.exception(indErr)
                self.logger.error("Cannot generate RA Data. Program will close")
                exit(1)
            except Exception as ex:
                self.logger.exception(ex)
                self.logger.error("Cannot generate RA Data. Program will close")
                exit(1)
