import boto3
import json
from datetime import datetime
import calendar
import random
import time
import json
import sys
import uuid

import aws_kinesis_agg.aggregator

stream_name = 'eCommStream'
ecomm_file=sys.argv[1]
kinesis_client = boto3.client('kinesis', region_name='ap-northeast-1')

def send_aggregate_record(agg_record):
    pk, _, data = agg_record.get_contents()
    kinesis_client.put_record(StreamName=stream_name, Data=data, PartitionKey=pk)
kinesis_client = boto3.client('kinesis', region_name='ap-northeast-1')
kinesis_agg = aws_kinesis_agg.aggregator.RecordAggregator()
kinesis_agg.on_record_complete(send_aggregate_record)
    

def main():
    with open(ecomm_file, 'r', encoding='utf-8') as data:
        pk = str(uuid.uuid4())
        for record in data:
            print(record)
            kinesis_agg.add_user_record(pk, record)
        send_aggregate_record(kinesis_agg.clear_and_get())

main()
