import json
from pipelines import on_crawler_process_inserted, on_crawler_process_updated
import requests
import boto3
import uuid
import time
import hashlib

from redis.client import Redis
from redis_cluster import RedisCluster


redis_connection = None


def has_redis_connection():
    global redis_connection
    return isinstance(redis_connection, Redis)


def get_redis_connection():
    global redis_connection
    return redis_connection


def kill_redis_connection():
    global redis_connection
    redis_connection.close()


def parse_image(image):
    """Parse records incoming from Amazon DynamoDB Update"""

    crawler_thread = {}
    for key in image:
        for value_type in image[key]:
            raw_value = image[key][value_type]
            value = None
            if value_type == "S":
                value = str(raw_value)
            elif value_type == "N":
                value = int(raw_value)
            elif value_type == "BOOL":
                value = bool(raw_value)
            else:
                value = raw_value

            crawler_thread[key] = value

    return crawler_thread


def lambda_handler(event, context):
    global redis_connection

    dd = boto3.resource('dynamodb', region_name="eu-west-3")

    crawler_threads_table = dd.Table('crawler_threads')

    for record in event['Records']:
        crawler_process = parse_image(record['dynamodb']['NewImage'])
        if record['eventName'] == "INSERT":
            # Generate CrawlerThread

            requests.post('https://api2-scrapers.bebee.com/testcp',
                          json=crawler_process)
            # Generate crawler_thread
            on_crawler_process_inserted(crawler_process)

        if record['eventName'] == "UPDATE":
            on_crawler_process_updated(
                parse_image(record['dynamodb']['NewImage']),
                parse_image(record['dynamodb']['OldImage'])
            )

    return {
        'statusCode': 200,
        'body': 'ok'
    }


# on_crawler_process_inserted({
#     'links_scraper_crawler_engine': 'SCRAPER',
#     'domain': 'api.ipify.org',
#     'url': 'https://api.ipify.org/?format=json',
#     'crawler_threads_cnt:done_scraping_links': 0,
#     'url_id-process-index': '753656-1',
#     'total_scraped_jobs': 0,
#     'url_id': 753656,
#     'crawler_process_index': 1,
#     'total_scraped_links': 0,
#     'age': 1626371268
# }
# )
