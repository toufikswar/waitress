import argparse
import logging

from datetime import datetime
from classes.salesforce import Salesforce
from classes.parser import JsonParser
from classes.logger import Logger


def main():

    my_parser = argparse.ArgumentParser(description="Waitress, at your service...")

    my_parser.add_argument("-d",
                           "--delete",
                           action="store_true",
                           help="Delete the RA Library database")
    args = my_parser.parse_args()

    print(args.delete)

    logger = Logger()
    logger.set_level()
    logger.set_file_handler()
    logger.logger.info("Initiating RAs Salesforce Library Loader")

    jason_parser = JsonParser()
    jason_parser.parse_json_folder()
    df = jason_parser.df_all
    jason_parser.save_to_excel(df, "test_merge")

    salesforce = Salesforce()

    if args.delete:
        salesforce.delete_all_ras()

    salesforce.process_dataframe(df)


if __name__ == "__main__":
    main()
