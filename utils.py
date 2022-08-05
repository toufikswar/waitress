import json
import logging

from datetime import datetime
import pandas as pd

utils_logger = logging.getLogger('waitress.utils')


def load_config(path_to_json):
    """Load a JSON configuration profile.

    :param path_to_json: The path to the JSON config file

    :returns: A JSON with application config
    """
    with open(path_to_json, "r") as f:
        return json.load(f)


def save_to_excel(df, name):
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
            utils_logger.exception(indErr)
            utils_logger.error("Cannot generate RA Data. Program will close")
        except Exception as ex:
            utils_logger.exception(ex)
            utils_logger.error("Cannot generate RA Data. Program will close")