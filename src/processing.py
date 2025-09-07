import numpy as np
import pandas as pd
from loguru import logger
from config import LOCATION_COLS

def get_minor_index(data: pd.DataFrame):
    """Drp index of riders with age less than 18"""
    return data[data['age'].astype(float) > 18]

def get_six_star_index(data: pd.DataFrame):
    """Drop index of riders with 6 star ratings"""
    return data[data['ratings'].astype(float) < 6]

def rename_cols(data: pd.DataFrame):
    """Rename columns to snake_case"""
    return (
        data.rename(str.lower,axis=1)
        .rename(
            {
                "delivery_person_id" : "rider_id",
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
            },
            axis=1
        )
    )

def clean_location(data: pd.DataFrame, threshold: float = 1.0) -> pd.DataFrame:
    """Clean location column by setting values less than threshold to NaN"""
    return (
        data
        .assign(
            **{
                col: (
                    np.where(data[col] < threshold, np.nan, data[col].values)
                )  for col in LOCATION_COLS 
            }
        )
    )

def data_cleaning(data: pd.DataFrame):

    logger.info("Starting data cleaning process")
    return (
        data
        .pipe(rename_cols)                        # rename columns to snake_case
        .pipe(get_minor_index)                  # drop index of minor riders
        .pipe(get_six_star_index)               # drop index of six star rated drivers
        .drop(columns="id")
        .replace("NaN ",np.nan)                   # missing values in the data
        .pipe(
            # make all values in lower and strip if any extra spaces
            lambda x: x.assign(
                **{col: x[col].str.strip().str.lower() for col in x.select_dtypes(include='object').columns}
            )
        )
        .assign(
            # Extract city name from rider_id
            city_name = lambda x: x['rider_id'].str.split("res").str.get(0),
            # convert age to float
            age = lambda x: x['age'].astype(float),
            # convert ratings to float
            ratings = lambda x: x['ratings'].astype(float),
            # absolute values for location based columns
            restaurant_latitude = lambda x: x['restaurant_latitude'].abs(),
            restaurant_longitude = lambda x: x['restaurant_longitude'].abs(),
            delivery_latitude = lambda x: x['delivery_latitude'].abs(),
            delivery_longitude = lambda x: x['delivery_longitude'].abs(),
            # order date to datetime
            order_date = lambda x: pd.to_datetime(x['order_date'],dayfirst=True),
            # time based columns
            order_time = lambda x: pd.to_datetime(x['order_time'], format='mixed'),
            order_picked_time = lambda x: pd.to_datetime(x['order_picked_time'],format='mixed'),
            weather = lambda x: (
                x['weather']
                .str.replace("conditions ","")
                .str.lower()
                .replace("nan",np.nan)
            ),
            # multiple deliveries column
            multiple_deliveries = lambda x: x['multiple_deliveries'].astype(float),
            # target column modifications
            time_taken = lambda x: (
                x['time_taken']
                .str.replace("(min) ","")
                .astype(int)
            )            
        )
        .pipe(clean_location, threshold=1.0)      # clean location columns
        .reset_index(drop=True)                   # reset index after all the filtering
        .dropna(
            subset=[
                'rider_id','age','ratings','order_time','order_picked_time',
                'weather','traffic','vehicle_condition','type_of_order',
                'type_of_vehicle','time_taken'
            ]
        )        
    )
     



if __name__ == "__main__":
    clean_df = data_cleaning(pd.read_csv(r"E:\MLOPS Infra\mlops-projects\fooddelivery-domain\fooddelivery-datacleaning-service\data\swiggy.csv"))
    print(clean_df.head(5))
    print(clean_df.info())
