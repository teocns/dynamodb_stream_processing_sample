from typing import List
import boto3
from boto3.dynamodb.conditions import Attr, Key
import time

aws_access_key_id = 'AKIAQLRVICZQD6ZSXUW7'
aws_secret_access_key = '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'


class BatchWriteItemsModel:
    items: List[dict]
    table_name: str
    update_key: str

    def __init__(self, table_name) -> None:
        self.items = []
        self.table_name = table_name

    def perform(self):
        db = boto3.resource('dynamodb', region_name="eu-west-3",
                            aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        table = db.Table('crawler_processes')
        with table.batch_writer() as writer:
            for item in self.items:
                writer.put_item(item)

    def put(self, item: dict):
        self.items.append(item)
