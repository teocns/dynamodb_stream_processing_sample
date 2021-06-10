import json
import boto3
import uuid
import time


def lambda_handler(event, context):
    # TODO implement

    dd = boto3.resource('dynamodb', region_name="eu-west-3")
    crawler_threads_table = dd.Table('crawler_threads')

    for record in event['Records']:
        if record['eventName'] == "INSERT":
            # Generate CrawlerThread

            crawler_process = record['dynamodb']['NewImage']

            # Generate crawler_thread

            crawler_threads_table.put_item(
                Item={
                    'process_id': crawler_process['process_id'],
                    'thread_id':   str(uuid.uuid4()),
                    'domain': crawler_process['domain'],
                    'url': crawler_process['url'],
                    'crawler_engine': crawler_process['links_scraper_crawler_engine'],
                    'age_created': crawler_process['age'],
                    'age_inserted': int(time.time()),
                    'scraped_links': 0,
                    'scraped_links_duplicates': 0,
                    'scraped_jobs': 0,
                    'bytes_transferred': 0,
                    'action_type': 'SCRAPE_LINKS',
                    'crawler_engine': crawler_process['links_scraper_crawler_engine'],
                    'is_completed': 0
                }
            )

            # Handle updates

            # if record['eventName'] == "UPDATE":

            #     # Generate CrawlerThread

            #     crawler_process = record['NewImage']

            #     ## Generate crawler_thread

            #     crawler_threads_table.put_item(
            #         Item={
            #             'process_id': crawler_process['process_id'],
            #             'thread_id':   str(uuid.uuid4()),
            #             'domain': crawler_process['domain'],
            #             'url': crawler_process['url'],
            #             'crawler_engine': crawler_process['links_scraper_crawler_engine'],
            #             'age_created': crawler_process['age'],
            #             'age_inserted': int(time.time()),
            #             'scraped_links': 0,
            #             'scraped_jobs': 0,
            #             'action_type': 'SCRAPE_LINKS',
            #             'complexive_bytes_transferred': 0,
            #             'links_scraper_crawler_engine': crawler_process['links_scraper_crawler_engine'],
            #             'is_completed': 0
            #         }
            #     )

    return {
        'statusCode': 200,
        'body': 'ok'
    }
