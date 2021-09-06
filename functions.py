from SimpleDyDb.api import update_item
from SimpleDyDb.UpdateItemInstructions import UpdateItemInstructions
from helpers import get_md5
from models.domains_statistics import DomainStatistics
import boto3
from boto3.dynamodb.conditions import Attr, Key
import time
import uuid
from providers.ConfigProvider import ConfigProvider


crawler_threads_table = boto3.resource('dynamodb').Table('crawler_threads')
crawler_processes_table = boto3.resource('dynamodb').Table('crawler_processes')
domain_statistics_table = boto3.resource('dynamodb').Table('domains')
tracked_urls_table = boto3.resource('dynamodb').Table('tracked_urls')

def generate_main_thread_for_crawler_process(crawler_process) -> DomainStatistics:
    #print('Generating main thread for crawler_process')
    
    
    process_id = str(crawler_process.get('url_md5#cp_cnt'))
    user_id = crawler_process.get('user_id')
    
    crawler_threads_table.put_item(
        Item={
            'thread_id': '%s#%s' % (process_id,str(1)),
            'domain': crawler_process.get('domain'),
            'url': crawler_process.get('url'),
            #'url_md5': get_md5(crawler_process.get('url')),
            'crawler_engine': crawler_process.get('crawler_engine'),
            'age': int(time.time()),
            #'age_completed': None,
            'user_id': str(user_id),
            'is_not_completed': 1,
            'proxy': crawler_process.get('proxy'),
            'scrape': 'LINKS',
        },
        # ReturnValues="ALL"
    )


# def set_process_completed():
#     # Simply removes is_completed column lol

RECRAWLING_DELAY_DEFAULT = int(ConfigProvider.get_config('RECRAWLING_DELAY_DEFAULT'))


CACHED_DOMAINS_STATISTICS = {}

def get_domain_statistics(domain):
    return domain_statistics_table.query(
        KeyConditionExpression=Key('domain').eq(domain)
    ).get('Items',[{
        'domain': domain,
    }])[0]


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

    links = crawler_process.get('links',0)
    duplicates = crawler_process.get('duplicates',0)
    jobs = crawler_process.get('jobs',0)
    bytes = crawler_process.get('bytes',0)
    
    cp_cnt = int(crawler_process.get('url_md5#cp_cnt').split('#').pop())

    HAS_JOBS = jobs > 0

    ## RECRAWLING LOGIC GOES HERE
    next_crawl = int(time.time()) + RECRAWLING_DELAY_DEFAULT
    
    # Determine whether crawler process is failed, by reviewing the following conditions
    # has at least 20% of the threads are failed, it never gave jobs
    threads_failed_cnt = int(crawler_process.get('threads_failed_cnt',0) or 0)
    # Determine the % of threads_failed_cnt relative to the amount of "links"
    
    threads_failed_percentage = int(threads_failed_cnt / links * 100) if links > 0 else 100
    
    CRAWLER_PROCESS_FAILED = threads_failed_percentage > 20 and not HAS_JOBS

    
    ready = 1 
    print("UPDATING URL NOW")
    # Retrieve URL
    updates = {
        "cp_done_cnt":[0,1],
        "cp_last_done_age":int(time.time()),
        "cp_last_links":links,
        "cp_last_jobs":jobs,
        "cp_last_bytes":bytes,
        "jobs": [0, jobs],
        "cp_last_duplicates":duplicates,
        "ready":ready,
        "next_crawl":next_crawl,
        "updates": [0,1]
        #"crawler_engine":":crawler_engine",
    }

    deletes = [

    ]

    if CRAWLER_PROCESS_FAILED:
        updates.update({
            "cp_failed_cnt": [0,1],
            "has_failed_cp": [1],
            "has_failed_cp_userid": crawler_process.get('user_id'),
            "last_failed_cp_age": [int(time.time())]
        })
    else:
        deletes.extend([
          "has_failed_cp",
          "has_failed_cp_userid"
        ])
        

    if HAS_JOBS:
        updates.update({
            "last_crawler_engine_with_jobs" :crawler_process.get('crawler_engine'),
        })

        deletes.extend([
            "no_jobs_crawled_yet",
            "consecutive_crawls_with_no_jobs"
        ])
    else:
        # If it's the first crawler process (cp_cnt == 1), then set "no_jobs_crawled_yet" to 1
        if cp_cnt == 1:
            updates.update({"no_jobs_crawled_yet": 1})
        
        updates.update({
            "consecutive_crawls_with_no_jobs": [0,1]
        })

    
  
    update_item(
        tracked_urls_table
        Key={
            'url': crawler_process.get('url')
        },
        UpdateItemInstructions(
            updates,
            deletes
        )
    )

