import boto3


db = boto3.resource('dynamodb')
table = db.Table('tracked_urls')



#import python_dynamodb_lock

db = boto3.resource('dynamodb')

db.Table('crawler_processes').update_item(
    Key={
        'url_md5#cp_cnt': '54b6aa8f78ea9a49020e8e86a84721e1#6'
    },
    ExpressionAttributeValues ={
        ':threads_done_cnt': 212,
    },
    UpdateExpression = "SET threads_done_cnt = :threads_done_cnt"
)
