from simpledydb import generate_expressions
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
            'is_not_completed': 1,
            'proxy': crawler_process.get('proxy'),
            'scrape': 'LINKS',
        },
        # ReturnValues="ALL"
    )


# def set_process_completed():
#     # Simply removes is_completed column lol

RECRAWLING_DELAY_DEFAULT = ConfigProvider.get_config('RECRAWLING_DELAY_DEFAULT')



CACHED_DOMAINS_STATISTICS = {}

def get_domain_statistics(domain):
    db = boto3.resource('dynamodb')

    return db.Table('domains').query(
        KeyConditionExpression=Key('domain').eq(domain)
    ).get('Items')[0]


def inverse_dict(my_dict):
    """
    the func get a dictinary and reverse it, the keys become values and the values become keys.
    :param my_dict: the dictinary that need to be reversed.
    :return: a VERY pretty dictionary.
    """
    result_dict = {}
    for key, value in my_dict.items():
        if not value in result_dict.keys():
            result_dict[value] = []
        result_dict[value].append(key)
    return result_dict, print(result_dict)


def update_tracked_url_after_completion(crawler_process):

    db = boto3.resource('dynamodb')

    

    links = crawler_process.get('links',0)
    duplicates = crawler_process.get('duplicates',0)
    jobs = crawler_process.get('jobs',0)
    bytes = crawler_process.get('bytes,0')
    
    cp_cnt = int(crawler_process.get('url_md5#cp_cnt').split('#').pop())

    HAS_JOBS = jobs > 0

    ## RECRAWLING LOGIC GOES HERE
    next_crawl = int(time.time()) + RECRAWLING_DELAY_DEFAULT
    next_crawler_engine = "SCRAPER"

   

    
    

    CRAWLER_PROCESS_FAILED = crawler_process.get('is_failed',0) == 1


    ready = 1 

    


    # Retrieve URL
    updates = {
        "cp_done_cnt":[0,1],
        "cp_last_done_age":int(time.time()),
        "cp_last_links":links,
        "cp_last_jobs":jobs,
        "cp_last_bytes":bytes,
        "cp_last_duplicates":duplicates,
        "ready":ready,
        "next_crawl":next_crawl,
        "crawler_engine":":crawler_engine",
    }

    deletes = [

    ]

 

    if CRAWLER_PROCESS_FAILED:
        updates.update({
            "cp_failed_cnt": [0,1],
            "has_failed_cp": [1],
            "has_failed_cp_userid": [0,1],
            "last_failed_cp_age": [int(time.time())]
        })
    else:
        deletes.extend([
          "has_failed_cp",
          "has_failed_cp_userid"
        ])
        

    if HAS_JOBS:
        deletes.append(
            "no_jobs_crawled_yet",
            "consecutive_crawls_with_no_jobs"
        )
    else:
        # If it's the first crawler process (cp_cnt == 1), then set "no_jobs_crawled_yet" to 1
        if cp_cnt == 1:
            updates.update({"no_jobs_crawled_yet": 1})
        
        updates.update({
            "consecutive_crawls_with_no_jobs": [0,1]
        })


    update_expression_query, expression_attribute_names, expression_attribute_values= generate_expressions(updates,deletes)


    db.Table('tracked_urls').update_item(
        Key={
            'url': crawler_process.get('url')
        },
        UpdateExpression = update_expression_query,
        ExpressionAttributeValues = expression_attribute_values,
        ExpressionAttributeNames = expression_attribute_names
    )


    
    # db.Table('domains').update_item(
    #     Key={
    #         'domain': crawler_process.get('domain')
    #     },
    #     UpdateExpression = "SET TOTAL_SCRAPED_JOBS_CNT = ",
    #     ExpressionAttributeValues = update_values,
    #     ExpressionAttributeNames = {
    #         '#cp_done_cnt': 'cp_done_cnt',
    #         '#cp_last_done_age': 'cp_last_done_age',
    #         '#cp_last_links': 'cp_last_links',
    #         '#cp_last_jobs': 'cp_last_jobs',
    #         '#cp_last_bytes': 'cp_last_bytes',
    #         '#cp_last_duplicates': 'cp_last_duplicates',
    #         "#ready": 'ready',
    #         "#next_crawl": 'next_crawl',
    #     }
    # )


    





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
