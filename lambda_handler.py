import json
import boto3


def lambda_handler(event, context):
    # TODO implement
    dd = boto3.resource('dynamodb', region_name="eu-west-3")

    t = dd.Table('crawler_processes')

    for record in event['Records']:
        if record['']

    return {
        'statusCode': 200,
        'body': 'ok'
    }
