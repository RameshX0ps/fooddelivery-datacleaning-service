import logging
from loguru import logger

from .db import DatabaseRepository
from .processing import data_cleaning
from .config import settings

def main():
    logger.info("Starting data cleaning job")
    repo = DatabaseRepository()
    query = f"SELECT * FROM {settings.RAW_DB_TABLE_NAME}"
    raw_df = repo.fetch_dataframe(query)
    if raw_df.empty:
        logger.warning("No rows found in raw table.")
        return
    cleaned = data_cleaning(raw_df)
    repo.push_dataframe(cleaned, settings.CLEANED_DB_TABLE_NAME)
    logger.info("Finished data cleaning job")

if __name__ == "__main__":
    main()
