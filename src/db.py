from typing import Optional
import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from .config import DATABASE_URL

engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800,
    future=True,
    echo=False,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

class DatabaseRepository:
    def __init__(self):
        self.engine = engine

    def fetch_dataframe(self, query: str, params: Optional[dict] = None) -> pd.DataFrame:
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

    def push_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str = "public",
        if_exists: str = "replace",
        index: bool = False,
        chunksize: int = 1000,
    ) -> None:
        if df.empty:
            raise ValueError("DataFrame is empty. Nothing to insert.")
        try:
            with self.engine.begin() as conn:
                df.to_sql(
                    name=table_name,
                    con=conn,
                    schema=schema,
                    if_exists=if_exists,
                    index=index,
                    chunksize=chunksize,
                    method="multi",
                )
            logger.info(f"Inserted {len(df)} rows into {schema}.{table_name}")
        except SQLAlchemyError as e:
            logger.error("Database insert failed", exc_info=e)
            raise
