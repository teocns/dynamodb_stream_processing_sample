import json
import boto3


def lambda_handler(event, context):
    # TODO implement

    dd = boto3.resource('dynamodb', region_name="eu-west-3")
    crawler_threads_table = dd.Table('crawler_threads')

    for record in event['Records']:
        if record['eventName'] == "INSERT":
            # Generate CrawlerThread

            crawler_process = record['NewImage']

            crawler_threads_table.put_item(
                Item={
                    'process_id': ''
                }
            )

    return {
        'statusCode': 200,
        'body': 'ok'
    }
