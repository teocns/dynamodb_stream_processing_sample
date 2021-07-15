import json
from pipelines import on_crawler_process_inserted, on_crawler_process_updated
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


def lambda_handler(event, context):
    global redis_connection

    dd = boto3.resource('dynamodb', region_name="eu-west-3")
    crawler_threads_table = dd.Table('crawler_threads')

    for record in event['Records']:
        if record['eventName'] == "INSERT":
            # Generate CrawlerThread

            crawler_process = record['dynamodb']['NewImage']
            # Generate crawler_thread
            on_crawler_process_inserted(crawler_process)

        if record['eventName'] == "UPDATE":
            on_crawler_process_updated(
                record['dynamodb']['NewImage'],
                record['dynamodb']['OldImage']
            )

    return {
        'statusCode': 200,
        'body': 'ok'
    }
