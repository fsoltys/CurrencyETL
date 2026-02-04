import os
import logging

logger = logging.getLogger(__name__)


class Config:
    """Configuration class for ETL process."""
    NBP_API_URL = "https://api.nbp.pl/api/exchangerates/tables/A/"

    @staticmethod
    def get_db_url() -> str:
        """Builds the postgresql connection string from environment variables.
        :returns str: PostgreSQL connection string."""

        user = os.getenv("POSTGRES_USER")
        password = os.getenv("POSTGRES_PASSWORD")
        host = os.getenv("POSTGRES_HOST")
        port = os.getenv("POSTGRES_PORT", "5432")
        db_name = os.getenv("POSTGRES_DB")

        if not all([user, password, host, db_name]):
            error_msg = "Database configuration environment variables are not fully set."
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.debug(f"Database URL constructed for user: {user} on host: {host}")

        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"