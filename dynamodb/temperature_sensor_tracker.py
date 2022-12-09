from __future__ import print_function

import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ses = boto3.client('ses')
email_address = 'YOUR_EMAIL'


    response=json.dumps(event)
    
    for record in event['Records']:
        if float(record['dynamodb']["NewImage"]["temperature"]["N"]) > 28:
            subject = 'High Temperature Detected - ' +  record['dynamodb']["NewImage"]["id"]["S"]
            body_text = 'High Temperature has been detected in warehouse:' +  record['dynamodb']["NewImage"]["id"]["S"] + '\nThe high temperature detected is: ' +  record['dynamodb']["NewImage"]["temperature"]["N"] \
                        + '\nThere could be a danger of fire in the warehouse.'
            
            print(body_text)
    ses.send_email(Source=email_address,
                   Destination={'ToAddresses': [email_address]},
                   Message={'Subject': {'Data': subject}, 'Body': {'Text': {'Data': body_text}}})
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

