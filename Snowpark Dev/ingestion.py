from snowflake.snowpark import Session
import pandas as pd
import json

def create_session():
    with open("creds.json", "r") as creds_file:
        creds = json.load(creds_file)
    
    connection_params = {
        "account": creds["account"],
        "user": creds["uname"],
        "password": creds["password"],
        "role": "DATA_DEV",
        "warehouse": "MY_WH"
        }

    session = Session.builder.configs(connection_params).create()
    
    return session
    

def write_location_data():
    session = create_session()
    session.use_schema("SNOWPARK_POC_DATA.PUBLIC")

    raw_data = pd.read_excel("data/location.xlsx", sheet_name="location")
    raw_data = session.createDataFrame(raw_data)
    
    raw_data.write.mode("overwrite").saveAsTable("location")


def write_order_details_data():
    session = create_session()
    session.use_schema("SNOWPARK_POC_DATA.PUBLIC")
    
    raw_data = pd.read_excel("data/order_detail.xlsx", sheet_name="order_detail")
    raw_data = session.createDataFrame(raw_data)
    
    raw_data.write.mode("overwrite").saveAsTable("order_detail")
    

