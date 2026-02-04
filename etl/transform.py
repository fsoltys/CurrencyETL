import logging
import datetime
from decimal import Decimal

logger = logging.getLogger(__name__)

class DataTransformer:
    def extract_missing_currencies(self, raw_data: list[dict], existing_codes: dict) -> list[dict]:
        """Identifies and extracts new currencies from API

        :param raw_data: List of raw currency data from API
        :param existing_codes: Dictionary of existing currency codes
        :return: List of new currency data dictionaries
        """
        missing_currencies = []

        known_codes = set(existing_codes.keys())

        logger.debug(
            f"Starting currency extraction. Input: {len(raw_data)} items from API, {len(known_codes)} existing codes in DB.")

        for item in raw_data:
            code = item['code']
            if code not in known_codes:
                logger.debug(f"New currency detected: {code} ({item['currency']})")
                missing_currencies.append({
                    'code': code,
                    'name': item['currency']
                })
                known_codes.add(code)

        if missing_currencies:
            logger.info(f"Identified {len(missing_currencies)} missing currencies to insert.")
        else:
            logger.debug("No new currencies found.")

        return missing_currencies

    def transform_to_facts(self, raw_data: list[dict], currency_map: dict, rate_date: datetime.date) -> list[dict]:
        """Transforms raw currency rate data into fact records

        :param raw_data: List of raw currency rate data from API
        :param currency_map: Dictionary mapping currency codes to IDs
        :param rate_date: Date for the currency rates
        :return: List of transformed fact records
        """
        fact_records = []

        logger.debug(
            f"Starting transformation to fact records. Input: {len(raw_data)} items from API, {len(currency_map)} known currencies.")

        for item in raw_data:
            code = item['code']
            currency_id = currency_map.get(code)
            if currency_id:
                fact_records.append({
                    'currency_id': currency_id,
                    'rate': Decimal(str(item['mid'])), # using decimal for precision
                    'date': rate_date
                })
            else:
                logger.warning(f"Currency code {code} not found in currency map; skipping.")

        logger.info(f"Transformed {len(fact_records)} fact records for date {rate_date}.")

        return fact_records