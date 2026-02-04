# NBP Currency ETL

Containerized ETL pipeline designed to ingest, transform, and warehouse currency exchange rates from the National Bank of Poland (NBP) API.

This project demonstrates a production-ready approach to batch processing, focusing on data integrity, idempotent operations, and dimensional modeling.

## Tech Stack

* **Core:** Python 3.9
* **Database:** PostgreSQL (Star Schema Design)
* **ORM/Data:** SQLAlchemy, Psycopg2
* **Infrastructure:** Docker, Docker Compose
* **Orchestration:** Containerized Cron Scheduler

## Architecture & Pipeline Workflow

The pipeline follows a modular architecture separating extraction, transformation, and loading logic:

1.  **Extraction:**
    * Fetches daily exchange rate tables (Table A) from the NBP API.
    * Implements error handling for API availability checks (timeouts, 404s).

2.  **Transformation:**
    * **Data Typing:** Enforces `Decimal` types for all monetary values to prevent floating-point precision errors.
    * **Dimension Check:** Dynamically detects new currencies not present in the `dim_currency` table.

3.  **Loading:**
    * **Star Schema:** Loads data into a Fact/Dimension model (`fact_exchange_rate`, `dim_currency`).
    * **Idempotency:** Uses `ON CONFLICT DO NOTHING` strategies. The pipeline can be re-run multiple times for the same date without creating duplicate records or causing primary key violations.
  
## Getting Started

### Prerequisites
* Docker & Docker Compose

### Running the Pipeline
1.  Start the services:
    ```bash
    docker-compose up --build -d
    ```

2.  (Optional) Run a manual backfill for a specific date:
    ```bash
    docker exec -it nbp_etl_cron python /app/main.py --date 2024-01-01
    ```

3.  Monitor execution logs:
    ```bash
    docker logs -f nbp_etl_cron
    ```
