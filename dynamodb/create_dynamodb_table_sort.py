import boto3

dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')

mytable = dynamodb.create_table(
TableName= 'Temperature',
KeySchema=[
{
'KeyType': 'HASH',
'AttributeName': 'id'
},
{
'KeyType': 'RANGE',
'AttributeName': 'timestamp'
}

],
AttributeDefinitions=[
{
'AttributeName': 'id',
'AttributeType': 'S'
},
{
'AttributeName': 'timestamp',
'AttributeType': 'N'
}
],
ProvisionedThroughput={
'ReadCapacityUnits': 1,
'WriteCapacityUnits': 1
}
)
# Wait until the table creation complete.
mytable.meta.client.get_waiter('table_exists').wait(TableName='Temperature')
print('Table has been created, please continue to insert data.')
