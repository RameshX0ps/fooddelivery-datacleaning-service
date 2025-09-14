from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List

class Settings(BaseSettings):
    SWIGGY_POSTGRES_DB_PORT: int = Field(5432, env="SWIGGY_POSTGRES_DB_PORT")
    SWIGGY_POSTGRES_DB_NAME: str = Field("swiggy", env="SWIGGY_POSTGRES_DB_NAME")
    SWIGGY_POSTGRES_DB_USER: str = Field("postgres", env="SWIGGY_POSTGRES_DB_USER")
    SWIGGY_POSTGRES_DB_HOST: str = Field("localhost", env="SWIGGY_POSTGRES_DB_HOST")
    SWIGGY_POSTGRES_DB_PASSWORD: str = Field("secret123", env="SWIGGY_POSTGRES_DB_PASSWORD")

    RAW_DB_TABLE_NAME: str = "raw_food_delivery_data"
    CLEANED_DB_TABLE_NAME: str = "cleaned_delivery_data"

    LOCATION_COLS: List[str] = [
        "restaurant_latitude",
        "restaurant_longitude",
        "delivery_latitude",
        "delivery_longitude",
    ]
    DROP_COLUMNS:List[str] =  [
        'rider_id',
        'restaurant_latitude',
        'restaurant_longitude',
        'delivery_latitude',
        'delivery_longitude',
        'order_date',
        "order_time_hour",
        "order_day",
        "city_name",
        "order_day_of_week",
        "order_month"
    ]

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()

DATABASE_URL = (
    f"postgresql+psycopg2://{settings.SWIGGY_POSTGRES_DB_USER}:"
    f"{settings.SWIGGY_POSTGRES_DB_PASSWORD}@"
    f"{settings.SWIGGY_POSTGRES_DB_HOST}:{settings.SWIGGY_POSTGRES_DB_PORT}/"
    f"{settings.SWIGGY_POSTGRES_DB_NAME}"
)
