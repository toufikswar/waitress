import os
import json
import logging
import pandas as pd

from datetime import datetime

module_logger = logging.getLogger('waitress.parser')


class JsonParser:
    """JsonParser class.

    Class that manages all the parsing, data extraction, and Remote Action listing. It consolidates different
    sources before joining all required data into one data source used as single point of truth.
    """

    def __init__(self, config_file):
        """JsonParser constructor.

        Contains:
            * logger instantiation
            * call to :code:`_load_config()`
            * call to :code:`_load_data_categories()`
            * creation of :code:`_df_json`
            * creation of :code:`_df_all`
            * creation of :code:`list_ra_names`
        :param config_file: a path to a JSON config file
        :type: config_file: str
        """
        self.logger = logging.getLogger("waitress.parser.JsonParser")  # Initialize the logger object
        self._load_config(config_file)  # Load configuration (from JSON file)

        self.logger.info(f"Initiating the JSON Parser Object - Will parse JSON files in {self._path_to_json}")
        self._df_category = self._load_data_categories()  # Load RAs categories from Excel file

        self._df_json = pd.DataFrame()  # Df to store data from JSON files
        self._df_all = pd.DataFrame()  # Df to merge JSON file data with Category Excel file

        self.list_ra_names = ["Name"]  # List to store the Names of the JSON RAs in the out/ folder

    def _load_config(self, config_file):
        """Loads the JSON config to the JsonParser object.

        Private method that loads the configuration provided as input param to the JsonParser object.
        It could be
            * a dev config
            * a prod config
        :param config_file: a path to a JSON config file
        :type: config_file: str
        :return: None
        """
        self.logger.info(f"Instantiating Parser Object with {config_file['env']} configuration")
        self._path_to_json = config_file["path_to_json"]
        self._remote_actions_metadata = config_file["remote_actions_metadata"]

    def _load_data_categories(self):
        """Loads RA and metadata

        Private method that loads the :code:`categories.xlsx` file

        :returns: a dataframe with data loaded from :code:`categories.xlsx`
        :rtype: pandas.DataFrame
        """
        try:
            self.logger.info(f"Loading RA metadata from {self._remote_actions_metadata}")
            df_category = pd.read_excel(self._remote_actions_metadata, index_col=0)
            df_category = df_category.astype({'Internal': bool})  # transform the Internal column to bool
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
        """Parse the RA repo.

         Public method that extracts metadata information from RA JSON files.

         Steps:
            * Loop through the JSON RA directory
            * Select only RA JSON files
            * Read the file
            * Add extracted data to a list
            * Add RA names to self.list_ra_names
         Populate the ra_list with the extract data, and the self.list_ra_names with only the RA names.
        :return: None
        """
        try:
            if not os.path.exists(self._path_to_json):
                self.logger.error(f"JSON Folder {self._path_to_json} do not exists. Make sure it does.")
                exit(1)
            ra_list = []
            for root, dirs, files in os.walk(self._path_to_json):  # Loop on folder containing the JSON RAs
                for filename in files:
                    file_fullpath = os.path.join(root, filename)
                    if filename.endswith(".json"):  # Make sure we select a JSON
                        returned_dict = self._read_json_file(file_fullpath)  # We read the RA JSON
                        if returned_dict:
                            ra_list.append(returned_dict)  # Populate the RA list with all data
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
                self._df_json = pd.DataFrame(ra_list)  # create a dataframe with the data gathered
            else:
                self.logger.warning("No JSON files found in the specified location. Waitress will exit.")
                exit(0)

    @property
    def ra_json_names(self):
        """Get the names of the RA from the JSON files

        :return: a dataframe with RA Names
        :rtype: pandas.DataFrame
        """
        if not self._df_json.empty:
            return self._df_json.loc[:, ["Name"]]

    def get_delta_dataframe(self):
        """Public method that returns the delta

        :return: a dataframe that have a JSON file but not yet listed in categories.xlsx
        :rtype: pandas.DataFrame
        """
        df_json = self.ra_json_names
        df_category_names = self.df_category.loc[:, ["Name"]]
        df_delta = pd.concat([df_json, df_category_names]).drop_duplicates(keep=False)
        return df_delta

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
        """Create a full dataframe.

        Create a data frame by making a left join between categories.xlsx RAs with JSON parsing folder RAs.
        The data frame consists on the source of truth to populate the Library.
        Since it's a left join the data that is kept is **categories.xlsx**

        :returns: A dataframe
        """
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
        """Open and extract RA metadata from a RA JSON file.

        Open the JSON file and extract the Name, Description, Purpose, Type, Path

        :param file_path: The path of the JSON RA
        :return: Dictionary
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
        """Assign a type to a RA.

        Read the JSON fields and assigned a type to the RA that can be:
            * Windows
            * macOS
            * Combined
            * ""

        :param json_obj:
        :return: a string with RA type
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
