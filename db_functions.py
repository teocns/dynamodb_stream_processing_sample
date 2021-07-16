from helpers import get_md5
from models.domains_statistics import DomainStatistics
import boto3
from boto3.dynamodb.conditions import Attr, Key
import time
import uuid

aws_access_key_id = 'AKIAQLRVICZQD6ZSXUW7'
aws_secret_access_key = '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'


def get_domains_statistics(domain) -> DomainStatistics:
    db = boto3.resource('dynamodb', region_name="eu-west-3",
                        aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    table = db.Table('domains_statistics')
    return table.get_item(
        Key={
            'domain': domain
        }
    ).get('Item')


def generate_main_thread_for_crawler_process(crawler_process) -> DomainStatistics:
    db = boto3.resource('dynamodb', region_name="eu-west-3",
                        aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    table = db.Table('crawler_threads')

    table.put_item(
        Item={
            'url_id-process-index': str(crawler_process.get('url_id-process-index')),
            'thread_id':   str(uuid.uuid4()),
            'domain': crawler_process.get('domain'),
            'url': crawler_process.get('url'),
            'url_md5': get_md5(crawler_process.get('url')),
            'crawler_engine': crawler_process.get('crawler_engine'),
            'age': int(time.time()),
            #'age_completed': None,
            'is_completed': 0,
            'links': 0,
            'duplicates': 0,
            'jobs': 0,
            'bytes': 0,
            'scrape': 'LINKS',
        },
        # ReturnValues="ALL"
    )


# def ban_tracked_url(url_id, reason):
#     db = boto3.resource('dynamodb', region_name="eu-west-3",
#                         aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

#     table = db.Table('tracked_urls')

#     # TODO


# db = boto3.resource('dynamodb', region_name="eu-west-3",
#                     aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# table = db.Table('crawler_processes')


# table.update_item(
#     Key={
#         'url_id-process-index': 'asdasdsad',
#     },
#     UpdateExpression="SET asd = if_not_exists(asd, :start) + :inc",
#     ExpressionAttributeValues={
#         ':inc': 1,
#         ':start': 0,
#     },
#     ReturnValues="UPDATED_NEW"
# )
