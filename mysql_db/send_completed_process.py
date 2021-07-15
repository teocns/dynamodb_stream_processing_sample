# Use this code snippet in your app.
# If you need more information about configurations or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developers/getting-started/python/
from api.mysql_proxy import send_query
import time
import boto3
import base64
from botocore.exceptions import ClientError


def send(crawler_process):
    query = {
        'query': """call on_crawler_process_completed(%s,%s,%s,%s,%s,%s)""",
        'values': [
            crawler_process.get('url_id'),
            crawler_process.get('jobs'),
            crawler_process.get('links'),
            crawler_process.get('duplicates'),
            crawler_process.get('bytes'),
            crawler_process.get('crawler_engine')
        ]
    }

    success = False

    # TODO ADD MAX RETRIES
    while not success:
        try:
            success = send_query(query)
        except:
            print('Retrying in 1 minute')
            time.sleep(1)
