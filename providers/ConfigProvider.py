from config import api_endpoint
import os
import requests
import boto3

class ConfigProvider:

    @staticmethod
    def get_config(config_name):
        dydb = boto3.resource('dynamodb')
        table = dydb.Table('crawler_config')


        items =  table.query(
            ExpressionAttributeValues={
                ":val" : config_name
            },
            ExpressionAttributeNames={
                "#name" : 'name'
            },
            KeyConditionExpression='#name = :val',
        ) 

        if len(items['Items']):
            item = items['Items'].pop()
            return item.get('value')
        
