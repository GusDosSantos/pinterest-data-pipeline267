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
    for record in data['records']:
        record['value'] = {k: v.isoformat() if isinstance(v, datetime.datetime) else v for k, v in record['value'].items()}

    response = requests.post(url, headers=headers, data=json.dumps(data))
    return response.status_code

def run_data_loop():
    random.seed(100)
    connector = AWSDBConnector()

    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

    while True:
        random_row = random.randint(0, 11000)
        pin_query = f"SELECT * FROM pinterest_data LIMIT {random_row}, 1"
        geo_query = f"SELECT * FROM geolocation_data LIMIT {random_row}, 1"
        user_query = f"SELECT * FROM user_data LIMIT {random_row}, 1"

        pin_data = fetch_data(connector, pin_query)
        geo_data = fetch_data(connector, geo_query)
        user_data = fetch_data(connector, user_query)

        for pin_result, geo_result, user_result in zip(pin_data, geo_data, user_data):
            pin_status = post_to_api(
                "https://f4fw24ytg2.execute-api.us-east-1.amazonaws.com/test/topics/12f6caff3559.pin",
                headers,
                {"records": [{"value": pin_result}]}
            )
            geo_status = post_to_api(
                "https://f4fw24ytg2.execute-api.us-east-1.amazonaws.com/test/topics/12f6caff3559.geo",
                headers,
                {"records": [{"value": geo_result}]}
            )
            user_status = post_to_api(
                "https://f4fw24ytg2.execute-api.us-east-1.amazonaws.com/test/topics/12f6caff3559.user",
                headers,
                {"records": [{"value": user_result}]}
            )

            print(pin_status, geo_status, user_status)

if __name__ == "__main__":
    run_data_loop()
