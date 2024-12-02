import logging
import os
import argparse
import time
import yaml
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path
from utils.commons import access_token_authenticate, get_tableau_metadata

env_path = Path("config/.env")
load_dotenv(dotenv_path=env_path)
logging.basicConfig(level=logging.INFO)

personal_access_token_name = os.getenv("TABLEAU_PERSONAL_ACCESS_TOKEN_NAME")
personal_access_token_secret = os.getenv("TABLEAU_PERSONAL_ACCESS_TOKEN_SECRET")

server = os.getenv("TABLEAU_SERVER")
server_name = os.getenv("TABLEAU_SERVER_NAME")
site_id = os.getenv("TABLEAU_SITE_ID")
version = os.getenv("TABLEAU_VERSION")

with open("config/config.yml", 'r') as f:
    app_config = yaml.safe_load(f)


def main():
    """
    Main function to query the Tableau Server and save the metadata of the workbooks in a JSON and CSV file.
    """
    parser = argparse.ArgumentParser(
        description=
        "Use the metadata API to get information on a published data source.")
    
    parser.add_argument(
        '--lookback_days',
        type=int,
        default=180,
        help='Number of lookback days to process the hyper file from the tdsx file (default: 90)'
    )
    args = parser.parse_args()
    lookback_days = args.lookback_days
    
    lookback_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
    logging.info(f"Processing the hyper file from the tdsx file for the last {lookback_days} days")
    logging.info(f"Lookback date for hyper event logs: {lookback_date}")

    start_time = time.time()

    with open("config/query.txt", "r") as file:
        query = file.read()

    server_tsc, tableau_auth = access_token_authenticate(
        personal_access_token_name, personal_access_token_secret, site_id,
        server, version)

    server_tsc.use_server_version()

    get_tableau_metadata(server_tsc, tableau_auth, query, app_config, lookback_date)
    
    end_time = time.time()
    elapsed_time = str(timedelta(seconds=round(end_time - start_time)))
    logging.info("Duration {}".format(elapsed_time))

if __name__ == "__main__":
    # python main.py --lookback_days 90
    main()
