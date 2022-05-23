import os
import pandas as pd
import yaml
import json
from datetime import datetime
from os.path import exists
from decouple import config

PATH_TO_REPO = config("PATH_TO_REPO")
JSON_RAS_PATH = config("PATH_TO_JSON")


def main():
    list_ra_paths = []

    combined_ras = []

    """ Parse all files in PATH_TO_REPO.
    We store the results in a list list_ra_paths.
    """
    for root, dirs, files in os.walk(PATH_TO_REPO):
        for filename in files:
            file_fullpath = os.path.join(root, filename)
            if filename.endswith(".yaml"):
                if "combined" in filename.lower():
                    combined_ras.append(filename)
            if filename.endswith(".sh"):  # Check if file is a bash script
                list_ra_paths.append((filename, file_fullpath, "macOS"))  # Add the full of macOS RA
            if filename.endswith(".ps1"):  # Check if file is a PowerShell script
                if not filename.endswith(".Tests.ps1"):  # Discard Test .ps1 files
                    list_ra_paths.append((filename, file_fullpath, "Windows"))  # Add the full path

    print(combined_ras)
    df = pd.DataFrame()  # Create an empty dataframe to store RA info

    """ Loop over all the RAs path and fill the dataframe with information
    gathered from the YAML file.
    An ra element of list_ras is a tuple of (filename, filepath, OS version)
    """
    list_ras = list()  # Empty list to append our Win/macOS RAs
    for ra in list_ra_paths:
        if exists(ra[1]):  # Check if the RA path exists
            fullpath_no_ext, ext = os.path.splitext(ra[1])  # Remove the extension from the path
            fullpath_yaml = fullpath_no_ext + ".yaml"  # Add a new extension .yaml
            fullpath_json = JSON_RAS_PATH + ra[0] + ".json"  # Add a new extension .json
            if exists(fullpath_yaml):  # If the yaml file exists
                with open(fullpath_yaml) as file:  # Open the yaml file and extract the data
                    ra_description = yaml.full_load(file)
                    #  print(json.dumps(ra_description, indent=4, sort_keys=True))
                    value_to_add = {"RA Name": ra_description["action"]["name"],
                                    "Synopsis": ra_description["action"]["synopsis"],
                                    'OS': ra_description["action"]["platform"] if "platform" in
                                    ra_description["action"] else ra[2],  # Add OS version from tuple
                                    'Version': ra_description["action"]["version"][0],
                                    'Script file name' : ra[0],
                                    'Origin Path': ra[1],
                                    'JSON Path': fullpath_json
                                    }
                    list_ras.append(value_to_add)

            else:  # if no yaml files found
                # Get the RA name from the path
                value_to_add = {"RA Name": ra[0],  # Add the filename TODO Remove extension
                                "Synopsis": "NA",
                                'OS': "NA",
                                'Version': "NA",
                                'Origin Path': ra[1],
                                'JSON Path': fullpath_json
                                }
                list_ras.append(value_to_add)

    df = pd.DataFrame(list_ras)

    now = datetime.now()
    date_now = now.strftime("%m-%d-%Y-%H-%M-%S")

    excel_filename = f"./data/all_remote_actions_{date_now}.xlsx"

    with pd.ExcelWriter(excel_filename) as writer:
        df.to_excel(writer, sheet_name="all_remote_actions")


if __name__ == "__main__":
    main()
