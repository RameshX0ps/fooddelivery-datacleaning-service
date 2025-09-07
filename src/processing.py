import numpy as np
import pandas as pd
from loguru import logger
from config import settings
from schemas import raw_schema, cleaned_schema

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize dataframe column names to snake_case lowercase."""
    return df.rename(columns=lambda c: c.strip().lower())

def clean_location(df: pd.DataFrame, threshold: float = 1.0) -> pd.DataFrame:
    for col in settings.LOCATION_COLS:
        if col in df.columns:
            df[col] = df[col].mask(df[col] < threshold, np.nan)
    return df


def data_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting data cleaning process")

    # 1) Normalize column names to lowercase
    df = _normalize_columns(df)

    # 2) Validate raw schema (coercion will cast types where possible)
    df = raw_schema.validate(df)

    # Rename columns
    df = df.rename(
        columns={
            "delivery_person_id": "rider_id",
            "delivery_person_age": "age",
            "delivery_person_ratings": "ratings",
            "delivery_location_latitude": "delivery_latitude",
            "delivery_location_longitude": "delivery_longitude",
            "time_orderd": "order_time",
            "time_order_picked": "order_picked_time",
            "weatherconditions": "weather",
            "road_traffic_density": "traffic",
            "city": "city_type",
            "time_taken(min)": "time_taken"
        }
    )

    # Remove minors and invalid ratings
    df = df[df["age"].astype(float) > 18]
    df = df[df["ratings"].astype(float) < 6]
    logger.debug(f"Data shape after removing minors and invalid ratings: {df.shape}")    

    for col in df.select_dtypes(include="object"):
        df[col] = df[col].str.strip().str.lower()
    logger.debug("Trimmed whitespace and standardized case in string columns")
    logger.debug(f"Columns after normalization: {df.columns.tolist()}")

    df = df.assign(
        city_name=df["rider_id"].str.split("res").str.get(0),
        age=df["age"].astype(float),
        ratings=df["ratings"].astype(float),
        restaurant_latitude=df["restaurant_latitude"].abs(),
        restaurant_longitude=df["restaurant_longitude"].abs(),
        delivery_latitude=df["delivery_latitude"].abs(),
        delivery_longitude=df["delivery_longitude"].abs(),
        order_date=pd.to_datetime(df["order_date"], dayfirst=True, errors="coerce"),
        order_time=pd.to_datetime(df["order_time"], errors="coerce"),
        order_picked_time=pd.to_datetime(df["order_picked_time"], errors="coerce"),
        weather=(df["weather"].str.replace("conditions ", "")
                              .str.lower()
                              .replace("nan", np.nan)),
        multiple_deliveries=df["multiple_deliveries"].astype(float),
        time_taken=df["time_taken"].str.replace("(min) ", "").astype(int),
    )
    df = clean_location(df, threshold=1.0)
    df.drop(columns=["rider_id", "id"], inplace=True, errors="ignore")
    df = df.replace("NaN ", np.nan)
    df.dropna(inplace=True)

    logger.debug(f"Post-cleaning data shape: {df.shape}")
    logger.debug(f"Columns after cleaning: {df.columns.tolist()}")
    logger.debug(f"Data types:\n{df.dtypes}")
    # 🔹 Validate cleaned schema
    df = cleaned_schema.validate(df)

    logger.info("Data cleaning process completed")
    return df.reset_index(drop=True)


if __name__ == "__main__":
    file_path = "data/swiggy.csv"
    raw_data = pd.read_csv(file_path)
    cleaned_data = data_cleaning(raw_data)
    print(cleaned_data.head())
    print(cleaned_data.dtypes)