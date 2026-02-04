import datetime
import requests
from requests.exceptions import HTTPError, Timeout
from config import Config
import logging

logger = logging.getLogger(__name__)


class NBPApiClient:
    def __init__(self):
        self.api_url = Config.NBP_API_URL

    def fetch_rates(self, date: datetime.date) -> list:
        """Fetches exchange rates from NBP API for a given date.

        :param date: date to fetch rates for
        :returns list: List of exchange rates
        """
        date_str = date.strftime('%Y-%m-%d')
        logger.info(f"Fetching exchange rates from NBP for date: {date_str}")

        url = f"{self.api_url}{date_str}/?format=json"

        logger.debug(f"Request URL: {url}")

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            rates = data[0]['rates']
            logger.info(f"Successfully fetched {len(rates)} rates for {date_str}.")
            return rates

        except HTTPError as e:
            if response.status_code == 404:
                logger.warning(f"No data available for date {date_str} (HTTP 404).")
                return []
            elif response.status_code == 400:
                logger.error(f"Bad request sent to NBP: {url}.")
                raise e
            else:
                logger.error(f"HTTP error occurred while fetching NBP data: {e}")
                raise e

        except Timeout:
            logger.error(f"Request to NBP timed out after 10 seconds.")
            raise

        except Exception as e:
            logger.critical(f"An unexpected error occurred: {e}")
            raise