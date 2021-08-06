from helpers import get_md5
from models.domains_statistics import DomainStatistics
import boto3
from boto3.dynamodb.conditions import Attr, Key
import time
import uuid
from providers.ConfigProvider import ConfigProvider



def get_domains_statistics(domain) -> DomainStatistics:
    db = boto3.resource('dynamodb')

    table = db.Table('domains_statistics')
    return table.get_item(
        Key={
            'domain': domain
        }
    ).get('Item')


def generate_main_thread_for_crawler_process(crawler_process) -> DomainStatistics:
    #print('Generating main thread for crawler_process')
    db = boto3.resource('dynamodb')

    table = db.Table('crawler_threads')
    process_id = str(crawler_process.get('url_md5#cp_cnt'))
    user_id = crawler_process.get('user_id')
    
    table.put_item(
        Item={
            'thread_id': '%s#%s' % (process_id,str(1)),
            'domain': crawler_process.get('domain'),
            'url': crawler_process.get('url'),
            #'url_md5': get_md5(crawler_process.get('url')),
            'crawler_engine': crawler_process.get('crawler_engine'),
            'age': int(time.time()),
            #'age_completed': None,
            'user_id': user_id,
            'is_completed': 0,
            'links': 0,
            'duplicates': 0,
            'jobs': 0,
            'bytes': 0,
            'scrape': 'LINKS',
        },
        # ReturnValues="ALL"
    )


# def set_process_completed():
#     # Simply removes is_completed column lol

RECRAWLING_DELAY_DEFAULT = ConfigProvider.get_config('RECRAWLING_DELAY_DEFAULT')

def update_tracked_url_after_completion(crawler_process):
    db = boto3.resource('dynamodb')

    table = db.Table('tracked_urls')

    links = crawler_process.get('links')
    duplicates = crawler_process.get('duplicates')
    jobs = crawler_process.get('jobs')
    bytes = crawler_process.get('bytes')

    ## RECRAWLING LOGIC GOES HERE
    next_crawl = int(time.time()) + RECRAWLING_DELAY_DEFAULT
    ready = 1
    ##

    # Retrieve URL
    update_expressions = [
        "#cp_done_cnt = if_not_exists(#cp_done_cnt,:start) + :inc",
        "#cp_last_done_age = :tnow",
        "#cp_last_links = :links",
        "#cp_last_jobs = :jobs",
        "#cp_last_bytes = :bytes",
        "#cp_last_duplicates = :duplicates",
        "#ready = :ready",
        "#next_crawl = :next_crawl",

    ]
    
    update_values = {
        ':start' : 0,
        ':inc': 1,
        ':jobs': jobs,
        ':links': links,
        ':bytes': bytes, 
        ':duplicates': duplicates, 
        ':tnow': int(time.time()),
        ':ready': ready,
        ':next_crawl': next_crawl
    }
    
    update_expression_query = "SET " + ", ".join(update_expressions)


  
    table.update_item(
        Key={
            'url': crawler_process.get('url')
        },
        UpdateExpression = update_expression_query,
        ExpressionAttributeValues = update_values,
        ExpressionAttributeNames = {
            '#cp_done_cnt': 'cp_done_cnt',
            '#cp_last_done_age': 'cp_last_done_age',
            '#cp_last_links': 'cp_last_links',
            '#cp_last_jobs': 'cp_last_jobs',
            '#cp_last_bytes': 'cp_last_bytes',
            '#cp_last_duplicates': 'cp_last_duplicates',
            "#ready": 'ready',
            "#next_crawl": 'next_crawl',

        }
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
