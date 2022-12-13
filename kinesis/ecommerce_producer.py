import boto3
import json
from datetime import datetime
import calendar
import random
import time
import json
import sys

stream_name = 'eCommStream'
ecomm_file=sys.argv[1]
kinesis_client = boto3.client('kinesis', region_name='us-east-1')

file1=open(ecomm_file, 'r')
Lines = file1.readlines()
count = 0
for line in Lines:
    count += 1
    #print(line.strip())
    obj = json.loads(line)
    print(str(obj))
    put_response = kinesis_client.put_record(StreamName=stream_name,Data=json.dumps(obj),PartitionKey='1')