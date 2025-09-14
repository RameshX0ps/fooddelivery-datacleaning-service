import pandas as pd
from fooddelivery_datacleaning.processing import data_cleaning

def make_raw_sample():
    return pd.DataFrame(
        {
            "ID": [1],
            "Delivery_person_ID": ["res1234"],
            "Delivery_person_Age": ["25"],
            "Delivery_person_Ratings": ["4.5"],
            "Restaurant_latitude": [12.34],
            "Restaurant_longitude": [77.58],
            "Delivery_location_latitude": [12.35],
            "Delivery_location_longitude": [77.59],
            "Order_Date": ["01-01-2024"],
            "Time_Orderd": ["10:00:00"],
            "Time_Order_picked": ["10:10:00"],
            "Weatherconditions": ["Sunny"],
            "Road_traffic_density": ["Low"],
            "Vehicle_condition": ["3"],
            "Type_of_order": ["Food"],
            "Type_of_vehicle": ["Bike"],
            "multiple_deliveries": ["1"],
            "Festival": ["No"],
            "City": ["Urban"],
            "Time_taken(min)": ["(min) 15"]
        }
    )

def test_data_cleaning_basic():
    raw = make_raw_sample()
    cleaned = data_cleaning(raw)
    assert not cleaned.empty
    assert "rider_id" in cleaned.columns
    assert cleaned.loc[0, "age"] == 25.0
    assert cleaned.loc[0, "ratings"] == 4.5
    assert isinstance(cleaned.loc[0, "time_taken"], int)
