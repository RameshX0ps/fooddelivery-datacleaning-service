# schemas.py
import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check

# 🔹 Schema for raw DB table
raw_schema = DataFrameSchema({
    "delivery_person_id": Column(str, nullable=False),
    "delivery_person_age": Column(str, nullable=True),     # will convert to float later
    "delivery_person_ratings": Column(str, nullable=True), # will convert to float later
    "delivery_location_latitude": Column(float, nullable=True),
    "delivery_location_longitude": Column(float, nullable=True),
    "restaurant_latitude": Column(float, nullable=True),
    "restaurant_longitude": Column(float, nullable=True),
    "order_date": Column(str, nullable=True),   # parse to datetime later
    "time_orderd": Column(str, nullable=True),
    "time_order_picked": Column(str, nullable=True),
    "weatherconditions": Column(str, nullable=True),
    "road_traffic_density": Column(str, nullable=True),
    "city": Column(str, nullable=True),
    "multiple_deliveries": Column(str, nullable=True),
    "vehicle_condition": Column(int, nullable=True),
    "type_of_order": Column(str, nullable=True),
    "type_of_vehicle": Column(str, nullable=True),
    "time_taken(min)": Column(str, nullable=True),
})

# 🔹 Schema for cleaned dataset
cleaned_schema = DataFrameSchema({
    "age": Column(float, Check.ge(18), nullable=False),       # must be >= 18
    "ratings": Column(float, Check.le(5.0), nullable=False),  # ratings < 6
    "restaurant_latitude": Column(float, Check.ge(0), nullable=False),
    "restaurant_longitude": Column(float, Check.ge(0), nullable=False),
    "delivery_latitude": Column(float, Check.ge(0), nullable=False),
    "delivery_longitude": Column(float, Check.ge(0), nullable=False),
    "order_date": Column(pa.DateTime, nullable=False),
    "order_time": Column(pa.DateTime, nullable=False),
    "order_picked_time": Column(pa.DateTime, nullable=False),
    "weather": Column(str, nullable=True),
    "traffic": Column(str, nullable=True),
    "city_type": Column(str, nullable=True),
    "city_name": Column(str, nullable=True),
    "multiple_deliveries": Column(float, nullable=True),
    "vehicle_condition": Column(int, nullable=True),
    "type_of_order": Column(str, nullable=True),
    "type_of_vehicle": Column(str, nullable=True),
    "time_taken": Column(int, Check.ge(0), nullable=False),
})
