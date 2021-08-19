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
            'proxy': crawler_process.get('proxy'),
            'jobs': 0,
            'bytes': 0,
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




def update_tracked_url_after_completion(crawler_process):

    db = boto3.resource('dynamodb')

    table = db.Table('tracked_urls')

    links = crawler_process.get('links')
    duplicates = crawler_process.get('duplicates')
    jobs = crawler_process.get('jobs')
    bytes = crawler_process.get('bytes')
    

    tracked_url = db.Table('tracked_urls').query(
        KeyConditionExpression=Key('url').eq(crawler_process.get('url'))
    ).get('Items').pop()



    ## RECRAWLING LOGIC GOES HERE
    next_crawl = int(time.time()) + RECRAWLING_DELAY_DEFAULT
    next_crawler_engine = "SCRAPER"

    # If tracked_url has has no previous cp_done_cnt and crawler_process.jobs == 0, then set next_crawl to now
    if tracked_url.get('cp_done_cnt',0) == None and crawler_process.get('jobs',0) == 0:
        next_crawl = int(time.time())
        # And also retry with "SPIDER" crawler_engine
        next_crawler_engine = "SPIDER"
    

    SHOULD_BAN_URL = False
    if tracked_url.get('cp_cnt#SPIDER',0) >= 5 and tracked_url.get('jobs',0) == 0:
        SHOULD_BAN_URL = True
    

    CRAWLER_PROCESS_FAILED = crawler_process.get('is_failed',0) == 1


    ready = 1

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
        "#crawler_engine = :crawler_engine",

    ]

    expr_attribute_names = {
        '#cp_done_cnt': 'cp_done_cnt',
        '#cp_last_done_age': 'cp_last_done_age',
        '#cp_last_links': 'cp_last_links',
        '#cp_last_jobs': 'cp_last_jobs',
        '#cp_last_bytes': 'cp_last_bytes',
        '#cp_last_duplicates': 'cp_last_duplicates',
        "#ready": 'ready',
        "#next_crawl": 'next_crawl',
        "#crawler_engine": 'crawler_engine',
        "#ban": 'ban',
        "#ban_issue_timestamp": 'ban_issue_timestamp',
        "#banned_by": 'banned_by',
        "#ban_issue_reason": 'ban_issue_reason',
        "#ban_issued_by": 'ban_issued_by',
    }
    
    update_values = {
        ':start' : 0,
        ':inc': 1,
        ':jobs': jobs,
        ':links': links,
        ':bytes': bytes, 
        ':duplicates': duplicates, 
        ':tnow': int(time.time()),
        ':ready': ready,
        ':next_crawl': next_crawl,
        ':crawler_engine': next_crawler_engine
    }


    if SHOULD_BAN_URL:
        update_expressions.append("#ban = :ban")
        update_values[':ban'] = 1
        update_expressions.append("#ban_issue_timestamp = :tnow")
        # set #banned_by to "robot"
        update_expressions.append("#ban_issued_by = :ban_issued_by")
        update_values[':ban_issued_by'] = "ROBOT"
        update_expressions.append("#ban_issue_reason = :ban_issue_reason")
        update_values[':ban_issue_reason'] = "BAN_FOR_REPEATED_CRAWL_WITHOUT_JOBS"

    if CRAWLER_PROCESS_FAILED:
        update_expressions.append("#cp_failed_cnt = if_not_exists(#cp_failed_cnt,:start) + :inc")
        update_expressions.append("#has_failed_cp = if_not_exists(#has_failed_cp:inc)")
        update_expressions.append("#last_failed_cp_age = if_not_exists(#last_failed_cp_age:inc)")
        expr_attribute_names['#cp_failed_cnt'] = 'cp_failed_cnt'
        expr_attribute_names['#has_failed_cp'] = 'has_failed_cp'
        expr_attribute_names['#last_failed_cp_age'] = 'last_failed_cp_age'

    
    update_expression_query = "SET " + ", ".join(update_expressions)


    table.update_item(
        Key={
            'url': crawler_process.get('url')
        },
        UpdateExpression = update_expression_query,
        ExpressionAttributeValues = update_values,
        ExpressionAttributeNames = expr_attribute_names
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
