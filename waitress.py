import argparse
import logging

from datetime import datetime
from classes.salesforce import Salesforce
from classes.parser import JsonParser
from classes.logger import Logger


def main():

    my_parser = argparse.ArgumentParser(description="... Waitress, at your service ...")

    my_parser.add_argument("-d",
                           "--delete_only",
                           action="store_true",
                           help="Delete all the RA Library database and exit waitress")
    my_parser.add_argument("-fs",
                           "--from_scratch",
                           action="store_true",
                           help="Recreate the Library from scratch")
    my_parser.add_argument('-v',
                           '--verbose',
                           action='store_true',
                           help='Enable verbose for debugging')
    my_parser.add_argument('-s',
                           '--save',
                           action='store_true',
                           help='Save full dataframe in an Excel file')

    args = my_parser.parse_args()  # Parse arguments in command line

    logger = Logger(logging.DEBUG if args.verbose else logging.INFO)
    logger.set_handler(file=True)
    logger.logger.info("Initiating RAs Salesforce Library Loader")

    jason_parser = JsonParser()
    jason_parser.parse_json_folder()  # Parse the JSON folder to extract metadata

    df = jason_parser.df_all  # Get the full dataframe will all RA data

    if args.save:
        jason_parser.save_to_excel(df, "All_RA_Data")

    salesforce = Salesforce()  # Create a Salesforce object to manage API queries

    if args.delete_only:  # This is in case we only want to empty the Salesforce Library
        delete_status = salesforce.delete_all_ras()
        exit(0) if delete_status else exit(1)

    salesforce.process_dataframe(df,
                                 from_scratch=args.from_scratch)


if __name__ == "__main__":
    main()
