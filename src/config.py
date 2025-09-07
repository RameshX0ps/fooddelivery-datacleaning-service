
import os

LOCATION_COLS = [
    'restaurant_latitude',
    'restaurant_longitude',
    'delivery_latitude',
    'delivery_longitude'
]

CLEANED_DB_TABLE_NAME="cleaned_delivery_data"
RAW_DB_TABLE_NAME="raw_food_delivery_data"

DB_PORT = os.getenv("SWIGGY_POSTGRES_DB_PORT", "5432")
DB_NAME = os.getenv("SWIGGY_POSTGRES_DB_NAME", "swiggy")
DB_USER = os.getenv("SWIGGY_POSTGRES_DB_USER", "postgres")
DB_HOST = os.getenv("SWIGGY_POSTGRES_DB_HOST", "localhost")
DB_PASS = os.getenv("SWIGGY_POSTGRES_DB_PASSWORD", "secret123")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
