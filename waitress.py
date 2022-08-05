import argparse
import logging

from utils import load_config, save_to_excel

from classes.salesforce import Salesforce
from classes.parser import JsonParser
from classes.logger import Logger


def main():

    my_parser = argparse.ArgumentParser(description="*** Waitress, at your service ***")
    my_parser.add_argument('-c',
                           '--config',
                           required=True,
                           help='Path to a JSON Configuration file.')
    my_parser.add_argument("-d",
                           "--delete_only",
                           action="store_true",
                           help="Empty all the RA Library and exit.")
    my_parser.add_argument("-fs",
                           "--from_scratch",
                           action="store_true",
                           help="Delete and repopulate the RA Library from scratch.")
    my_parser.add_argument('-v',
                           '--verbose',
                           action='store_true',
                           help='Enable extra verbose for debugging')
    my_parser.add_argument('-i',
                           '--diff',
                           action='store_true',
                           help="Check what RAs have a JSON but don't Library and quits.")
    my_parser.add_argument('-ex',
                           '--export',
                           action='store_true',
                           help='Export existing SF Library to Excel file and quits.')

    args = my_parser.parse_args()  # Parse arguments in command line
    prog_config = load_config(args.config)  # Load configuration provided as argument

    logger = Logger(logging.DEBUG if args.verbose else logging.INFO)
    logger.set_handler(file=True)
    logger.logger.info("Initiating RAs Salesforce Library Loader")

    json_parser = JsonParser(prog_config)  # Initiate a JsonParser object
    json_parser.parse_json_folder()  # Parse the JSON folder to extract metadata

    df = json_parser.df_all  # Get the full dataframe will all RAs data from the categories.xlsx file

    salesforce = Salesforce(prog_config)  # Create a Salesforce object to manage API queries

    if args.delete_only:  # This is in case we only want to empty the Salesforce Library
        delete_status = salesforce.delete_all_ras()
        exit(0) if delete_status else exit(1)

    if args.diff:
        delta = json_parser.get_delta_dataframe()
        logger.logger.info(f"The delta between Repo and SF Library is {delta}")
        exit(0)

    if args.export:
        save_to_excel(salesforce.existing_records, "all_existing_sf_records")
        exit(0)

    salesforce.process_dataframe(df,
                                 from_scratch=args.from_scratch)


if __name__ == "__main__":
    main()
