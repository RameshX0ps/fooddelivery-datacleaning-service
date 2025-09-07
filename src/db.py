import os
import pandas as pd
from loguru import logger
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from config import DATABASE_URL

# Engine with connection pooling
engine = create_engine(
    DATABASE_URL,
    pool_size=10,         # connections in pool
    max_overflow=20,      # extra connections if pool is full
    pool_timeout=30,      # wait time before giving up
    pool_recycle=1800,    # recycle connections every 30 min
    echo=False            # set to True for SQL debug logging
)

# Session factory
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

def get_session():
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def fetch_dataframe(query: str, params: dict | None = None) -> pd.DataFrame:
    """
    Execute a SQL query and return results as a Pandas DataFrame.
    Example:
        df = fetch_dataframe("SELECT * FROM flights WHERE price > :p", {"p": 5000})
    """
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

def push_to_db(
    data: pd.DataFrame,
    table_name: str,
    schema: str | None = "public",
    if_exists: str = "append",
    index: bool = False,
    chunksize: int = 1000,
) -> None:
    """
    Push a pandas DataFrame to the database table.

    Args:
        data (pd.DataFrame): DataFrame to push.
        table_name (str): Target table name.
        schema (str | None): Schema name (if applicable).
        if_exists (str): What to do if table exists.
                         Options: {'fail', 'replace', 'append'}.
        index (bool): Whether to write DataFrame index as a column.
        chunksize (int): Number of rows per batch insert.

    Raises:
        ValueError: If DataFrame is empty.
        SQLAlchemyError: If DB insert fails.
    """
    if data.empty:
        raise ValueError("DataFrame is empty. Nothing to insert.")
    try:
        with engine.begin() as conn:  # begin ensures COMMIT/ROLLBACK automatically
            data.to_sql(
                name=table_name,
                con=conn,
                schema=schema,
                if_exists=if_exists,
                index=index,
                chunksize=chunksize,
                method="multi",
            )
        logger.info(f"✅ Successfully inserted {len(data)} rows into {schema+'.' if schema else ''}{table_name}")
    except SQLAlchemyError as e:
        logger.error(f"❌ Database insert failed: {e}")
        raise