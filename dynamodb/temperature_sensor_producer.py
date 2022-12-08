import boto3
import json
from datetime import datetime
import calendar
import random
import time
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

def put_to_table(sensor_id, temp, sensor_timestamp):
    payload = {
                'temp': str(temp),
                'timestamp': str(sensor_timestamp),
                'sensor_id': sensor_id
              }

    print(payload)

    table = dynamodb.Table('Temperature')
    response = table.put_item(
                              Item={
                                    "id": sensor_id,
                                    "timestamp": sensor_timestamp,
                                    "temperature": Decimal(temp)
                                   }
                             )


while True:
    temp = random.uniform(20, 35)
    sensor_timestamp = calendar.timegm(datetime.utcnow().timetuple())

    room = ['WAREHOUSE1','WAREHOUSE2', 'WAREHOUSE3', 'WAREHOUSE4', 'WAREHOUSE5']
    sensor_id = random.choice(room)

    put_to_table(sensor_id, str(round(temp,2)), sensor_timestamp)

    # wait for 5 second
    time.sleep(5)

