import argparse
import datetime
import logging
import sys

from config import Config
from database import DBManager
from nbp_api import NBPApiClient
from transform import DataTransformer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

def parse_args():
    """Parses command line arguments.
    Allows running the script for a specific date: python main.py --date YYYY-MM-DD
    If no argument is provided, defaults to today date."""

    parser = argparse.ArgumentParser(description="ETL process for currency exchange rates.")
    parser.add_argument(
        '--date',
        type=str,
        help='Date in YYYY-MM-DD format, defaults to today if not provided.',
        default=datetime.date.today().strftime('%Y-%m-%d')
    )
    return parser.parse_args()

def run_etl(target_date_str: str):
    """Main ETL orchestration function."""

    try:
        target_date = datetime.datetime.strptime(target_date_str, '%Y-%m-%d').date()
    except ValueError:
        logger.critical(f"Invalid date {target_date_str}, expected format YYYY-MM-DD.")
        sys.exit(1)

    logger.info(f"Starting ETL process for date: {target_date}")

    try:
        db_url = Config.get_db_url()
        db_manager = DBManager(db_url)
        api_client = NBPApiClient()
        transformer = DataTransformer()

        raw_data = api_client.fetch_rates(target_date)

        if not raw_data:
            logger.info(f"No exchange rate data available for {target_date}. ETL process completed.")
            return

        current_currency_map = db_manager.get_currency_map()
        new_currencies = transformer.extract_missing_currencies(raw_data, current_currency_map)
        if new_currencies:
            db_manager.insert_new_currencies(new_currencies)
            logger.info(f"Refreshing currency map after inserting new currencies.")
            current_currency_map = db_manager.get_currency_map()

        fact_records = transformer.transform_to_facts(raw_data, current_currency_map, target_date)
        db_manager.insert_exchange_rates(fact_records)

        logger.info(f"ETL process for date {target_date} completed successfully.")

    except Exception as e:
        logger.critical(f"ETL process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    args = parse_args()
    run_etl(args.date)


