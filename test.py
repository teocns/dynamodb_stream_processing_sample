import boto3


db = boto3.resource('dynamodb')
table = db.Table('tracked_urls')



import python_dynamodb_lock

lock_client = python_dynamodb_lock.python_dynamodb_lock.DynamoDBLockClient(db)


table.update_item(
    Key={
        'url': 'asdfsadfdsaf'
    },
    ExpressionAttributeValues ={
        ':zero': 0,
        ':one': 1,
    },
    UpdateExpression = "SET process_cnt = :zero + :one"
)
