import boto3

dynamodb = boto3.resource('dynamodb', region_name='ap-northeast-1')

mytable = dynamodb.create_table(
TableName= 'Temperature',
KeySchema=[
{
'KeyType': 'HASH',
'AttributeName': 'id'
}
],
AttributeDefinitions=[
{
'AttributeName': 'id',
'AttributeType': 'S'
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
