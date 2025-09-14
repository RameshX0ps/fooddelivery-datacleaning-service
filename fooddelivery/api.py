from fastapi import FastAPI
from loguru import logger

from .db import DatabaseRepository
from .processing import data_cleaning
from .config import settings

app = FastAPI(
    title="Food Delivery Data Cleaning API",
    version="0.1.0",
    description="API for cleaning raw food delivery data and storing results in PostgreSQL"
)


@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "ok"}


@app.post("/clean-data")
def clean_data_endpoint():
    """Trigger the cleaning pipeline via API call."""
    logger.info("API request received: starting data cleaning job")
    repo = DatabaseRepository()

    # Fetch raw data
    query = f"SELECT * FROM {settings.RAW_DB_TABLE_NAME}"
    raw_df = repo.fetch_dataframe(query)
    if raw_df.empty:
        logger.warning("No rows found in raw table")
        return {"status": "no_data", "rows": 0}

    # Clean data
    cleaned_df = data_cleaning(raw_df)

    # Push back to DB
    repo.push_dataframe(cleaned_df, settings.CLEANED_DB_TABLE_NAME)
    logger.info(f"Finished API cleaning job, inserted {len(cleaned_df)} rows")

    return {"status": "success", "rows": len(cleaned_df)}
