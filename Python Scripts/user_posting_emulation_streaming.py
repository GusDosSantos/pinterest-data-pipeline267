import random
import requests
import sqlalchemy
from sqlalchemy import text
import json
import datetime

class AWSDBConnector:
    def __init__(self):
        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306

    def create_db_connector(self):
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        return engine

def fetch_data(connector, query):
    with connector.create_db_connector().connect() as connection:
        result = connection.execute(text(query))
        return [dict(row._mapping) for row in result]

def post_to_api(url, headers, data):
    response = requests.put(url, headers=headers, data=json.dumps(data, default=str))
    return response.status_code

def run_infinite_post_data_loop():
    random.seed(100)
    connector = AWSDBConnector()

    headers = {'Content-Type': 'application/json'}

    while True:
        random_row = random.randint(0, 11000)
        pin_query = f"SELECT * FROM pinterest_data LIMIT {random_row}, 1"
        geo_query = f"SELECT * FROM geolocation_data LIMIT {random_row}, 1"
        user_query = f"SELECT * FROM user_data LIMIT {random_row}, 1"

        pin_result = fetch_data(connector, pin_query)[0]
        geo_result = fetch_data(connector, geo_query)[0]
        user_result = fetch_data(connector, user_query)[0]

        pin_status = post_to_api(
            "https://f4fw24ytg2.execute-api.us-east-1.amazonaws.com/test/streams/streaming-12f6caff3559-pin/record",
            headers,
            {"StreamName": "streaming-12f6caff3559-pin", "Data": pin_result, "PartitionKey": "test"}
        )
        geo_status = post_to_api(
            "https://f4fw24ytg2.execute-api.us-east-1.amazonaws.com/test/streams/streaming-12f6caff3559-geo/record",
            headers,
            {"StreamName": "streaming-12f6caff3559-geo", "Data": geo_result, "PartitionKey": "test"}
        )
        user_status = post_to_api(
            "https://f4fw24ytg2.execute-api.us-east-1.amazonaws.com/test/streams/streaming-12f6caff3559-user/record",
            headers,
            {"StreamName": "streaming-12f6caff3559-user", "Data": user_result, "PartitionKey": "test"}
        )

        print(pin_status, geo_status, user_status)

if __name__ == "__main__":
    run_infinite_post_data_loop()
