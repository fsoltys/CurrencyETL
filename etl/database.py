import logging
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


class DBManager:
    def __init__(self, db_url: str):
        """Initializes the DBManager with a database URL.
        :param db_url: Database connection string.
        """
        self.engine = None
        self.db_url = db_url

    def get_engine(self) -> Engine:
        """Creates a new SQLAlchemy engine or returns one if it already exists.
        :returns Engine: SQLAlchemy engine connected to the database.
        """
        if self.engine is None:
            logger.info("Initializing new database engine connection.")
            self.engine = create_engine(self.db_url)
        return self.engine

    def get_currency_map(self) -> dict:
        """Fetches all currencies from dim table and returns a mapping of currency code to currency id.
        :returns dict: Code -> ID e.g. {"USD": 1, "EUR": 2}
        """
        logger.debug("Fetching currency map from database.")
        query = text("SELECT currency_id, currency_code FROM dim_currency;")

        with self.get_engine().connect() as connection:
            result = connection.execute(query)
            currency_map = {row.currency_code: row.currency_id for row in result}

        logger.info(f"Loaded currency map with {len(currency_map)} currencies.")
        return currency_map

    def insert_new_currencies(self, new_currencies: list[dict]) -> None:
        """Inserts new currencies into the dim_currency table.
        :param new_currencies: List of dicts with new currencies.
        """
        if not new_currencies:
            logger.debug("No new currencies to insert.")
            return

        logger.info(f"Inserting {len(new_currencies)} new currencies into dim_currency.")

        query = text("""INSERT INTO dim_currency (currency_code, currency_name)
                        VALUES (:code, :name)
                        ON CONFLICT (currency_code) DO NOTHING;""")

        with self.get_engine().begin() as connection:
            connection.execute(query, new_currencies)

        logger.debug("Finished inserting new currencies.")

    def insert_exchange_rates(self, fact_records: list) -> None:
        """Inserts exchange rate records into the fact_exchange_rate table.
        """
        if not fact_records:
            logger.warning("Received empty list of exchange rates. Skipping insert.")
            return

        logger.info(f"Inserting {len(fact_records)} exchange rate records into facts table.")

        query = text("""INSERT INTO fact_exchange_rate (currency_id, rate, rate_date)
                        VALUES (:currency_id, :rate, :date)
                        ON CONFLICT (currency_id, rate_date) DO NOTHING;""")

        with self.get_engine().begin() as connection:
            connection.execute(query, fact_records)

        logger.debug("Finished inserting exchange rates.")