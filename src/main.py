import os
import boto3
import json
import uuid

from tables import Tables


def lambda_handler(event, context):
    s3 = boto3.client('s3')    
    my_bucket_objects = s3.list_objects(Bucket= 'wattcher-statements', Prefix='779151001243')['Contents']
    for obj in my_bucket_objects:
        with open('filename', 'wb') as data:
            obj_key = obj['Key']
            name = obj_key.split('/')[1]
            print(f"name: {name}")
            s3.download_file('wattcher-statements', obj_key, f'./tmp/{name}')

    path_statements = './tmp/'
    tablas = Tables(path_statements)
    
    df, TA0, TA1, TA2, TA3, TAH = tablas.tablas()

    TA0_json = json.loads(TA0.to_json(orient='records'))

    dynamodb = boto3.resource('dynamodb')

    item_table = {'item': TA0_json, 'table_name': 'AccountData' }

    dynamoTable = dynamodb.Table(item_table['table_name'])
    for record in TA0_json:
        item = {'_id': str(uuid.uuid1()) ,**record}
        print(f'record: {item}')
        dynamoTable.put_item(Item=item)

    return {'message': f'Hello {event['key1']}'}