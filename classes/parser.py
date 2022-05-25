import os
import json
import logging
import pandas as pd

from datetime import datetime
from decouple import config

module_logger = logging.getLogger('fuser.parser')


class JsonParser:

    PATH_TO_JSON = config("PATH_TO_JSON")
    EXCEL_FILE_CATEGORIES = config("EXCEL_FILE_CATEGORIES")

    def __init__(self):
        self.logger = logging.getLogger("fuser.parser.JsonParser")
        self.logger.info("Initiating the JSON Parser Object")
        self.logger.debug(f"JSON file located in {self.PATH_TO_JSON}")
        self._df_category = self._load_data_categories()  # Load RAs categories from Excel file

        self._df_json = pd.DataFrame()
        self._df_all = pd.DataFrame()

    def _load_data_categories(self):
        """

        :return:
        """
        try:
            df_category = pd.read_excel(self.EXCEL_FILE_CATEGORIES, index_col=0)
        except FileNotFoundError as fileErr:
            self.logger.exception(fileErr)
        except PermissionError as permErr:
            self.logger.exception(permErr)
        except Exception as err:
            self.logger.exception(err)
        else:
            self.logger.info(f"Categories successfully loaded from {self.EXCEL_FILE_CATEGORIES}")
            return df_category

    def parse_json_folder(self):
        ra_list = []
        for root, dirs, files in os.walk(self.PATH_TO_JSON):
            for filename in files:
                file_fullpath = os.path.join(root, filename)
                if filename.endswith(".json"):
                    returned_dict = self._read_json_file(file_fullpath)
                    ra_list.append(returned_dict)
        if ra_list:
            self._df_json = pd.DataFrame(ra_list)

    @property
    def df_json(self):
        if not self._df_json.empty:
            return self._df_json
        return

    @property
    def df_category(self):
        if not self._df_category.empty:
            return self._df_category
        return

    @property
    def df_all(self):
        if not self.df_category.empty and not self.df_json.empty:
            try:
                self._df_all = pd.merge(self.df_category, self.df_json, on="Name", how="left")
            except KeyError as keyErr:
                self.logger.exception(keyErr)
            else:
                return self._df_all

        logging.error("Cannot retrieve full Data frame. Check if all data was provided.")
        return

    def _read_json_file(self, file_path):  # TODO Add Try Catch for JSON management
        """
        Comment
        :param file_path: test
        :return:
        """
        if os.path.exists(file_path):
            try:
                with open(file_path) as json_file:  # Open the JSON file and extract the data
                    data = json.load(json_file)
                    local_dict = {
                        "Name": data.get("name"),
                        "Description": data.get("description"),
                        "Purpose": data.get("purpose"),
                        "Type": self._get_ra_type(data["scriptInfo"]),
                        "Path": file_path
                    }

            except FileNotFoundError as fileErr:
                self.logger.exception(fileErr)
            except PermissionError as permErr:
                self.logger.exception(permErr)
            except Exception as err:
                self.logger.exception(err)
            else:
                logging.info(f"{data['name']} JSON file parsed with success.")
                return local_dict
        else:
            return

    @staticmethod
    def _get_ra_type(json_obj):
        if json_obj.get("scriptWindows") and json_obj.get("scriptMacOs"):
            return "Combined"
        elif json_obj.get("scriptWindows") and json_obj.get("scriptMacOs") is None:
            return "Windows"
        elif json_obj.get("scriptWindows") is None and json_obj.get("scriptMacOs"):
            return "macOS"
        else:
            return "NA"

    def save_to_excel(self, df, name):
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
