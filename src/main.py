import os
import pandas as pd
from loguru import logger

from processing import data_cleaning
from db import push_to_db, fetch_dataframe
from config import RAW_DB_TABLE_NAME, CLEANED_DB_TABLE_NAME, DB_NAME


class UploadToDB:
    def __init__(self, schema: str = "public"):
        self.schema_name = schema
        self.data: pd.DataFrame | None = None
        self.clean_data: pd.DataFrame | None = None

    def load_and_clean_data(self) -> pd.DataFrame:
        """Load data from database."""    
        if DB_NAME is None:
            raise ValueError("DB_NAME environment variable is not set.")
        query = f"SELECT * FROM {RAW_DB_TABLE_NAME}"
        self.data = fetch_dataframe(query)
        if self.data.empty:
            raise ValueError("No data found in the database table.")
        logger.info(f"Data loaded from DB: {self.data.shape[0]} rows, {self.data.shape[1]} cols")

        # Clean the data
        logger.debug("Cleaning the data before pushing to DB, Shape of data before cleaning: "
                     f"{self.data.shape[0]} rows, {self.data.shape[1]} cols")
        try:
            self.clean_data = data_cleaning(self.data)        
            logger.info(f"Cleaning data: {self.clean_data.shape[0]} rows, {self.clean_data.shape[1]} cols")    
        except Exception as e:
            logger.error(f"Error during data cleaning: {e}")
            raise


    def data_pusher(self,table_name):        
        """Push data to the database table."""
        try:
            if self.clean_data is None or self.clean_data.empty:
                self.load_and_clean_data()
                logger.info(f"Data loaded: {self.clean_data.shape[0]} rows, {self.clean_data.shape[1]} cols")

            # Push data to database
            logger.info(f"Pushing data to DB: {DB_NAME}, Table: {table_name}, Schema: {self.schema_name}")
            push_to_db(self.clean_data, table_name,self.schema_name)
            logger.info("Data push to DB completed successfully.")
        except Exception as e:
            logger.error(f"Error pushing data to DB: {e}")
            raise


def main():
    uploader = UploadToDB()
    # Push train dataset
    uploader.data_pusher(table_name=CLEANED_DB_TABLE_NAME)
    
if __name__ == "__main__":
    main()