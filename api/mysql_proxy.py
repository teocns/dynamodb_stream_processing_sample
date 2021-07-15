import requests
from config import MYSQL_ENDPOINT


def send_query(query: dict):
    response = requests.post(MYSQL_ENDPOINT, json=query)
    if response.status_code != 200:
        raise "Failed send_query: expected status code 200"
    print('Success')
    return True
