import asyncio
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Attr, Key
from boto3.dynamodb.transform import TransformationInjector
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

from batch_writer.utils import reader

ACCOUNT_TABLE = 'ACCOUNTS'
DATE_FORMAT =  "%Y-%m-%d:%H:%M:%SZ"
SLEEP_DURATION_MS = 5e-3 

def get_utc_now():
    return datetime.utcnow().strftime(DATE_FORMAT)    

def get_dynamo_client():
    rds = boto3.client('dynamodb', endpoint_url='http://localhost:8000', region_name='us-east-1')
    return rds

def create_table():
    dynamodb = get_dynamo_client()

    try:
        table = dynamodb.create_table(
            TableName = ACCOUNT_TABLE,
            KeySchema = [
                 {'AttributeName': 'account_identifier', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'account_identifier', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput = {'ReadCapacityUnits': 100, 'WriteCapacityUnits': 100}
        )        
        print('created table {}.'.format(ACCOUNT_TABLE))
    except ClientError as e:
        # If there is the table already, the error will be skipped
        if e.response['Error']['Code'] != 'ResourceInUseException':
            raise e


def list_tables():
    ## For a Boto3 client ('client' is for low-level access to Dynamo service API)
    dynamodb = get_dynamo_client()
    response = dynamodb.list_tables()
    return response

def scan_last():
    client = get_dynamo_client()
    paginator = client.get_paginator('scan')
    operation_model = client._service_model.operation_model('Scan')
    trans = TransformationInjector(deserializer = TypeDeserializer())
        
    operation_parameters = {
        'TableName': ACCOUNT_TABLE        
    }

    items = []

    for page in paginator.paginate(**operation_parameters):
        has_last_key = 'LastEvaluatedKey' in page
        if has_last_key:
            last_key = page['LastEvaluatedKey'].copy()
        trans.inject_attribute_value_output(page, operation_model)
        if has_last_key:
            page['LastEvaluatedKey'] = last_key
        items.extend(page['Items'])
    return items    

def query_account(account_identifie):    
    client = get_dynamo_client()
    params = {
        'TableName': ACCOUNT_TABLE,
        'KeyConditionExpression': 'account_identifier = :account_identifier',
        'ExpressionAttributeValues': {
            ":account_identifier": {'S': account_identifier}            
        },
        'ScanIndexForward': False,
        'Limit': 3
    }
    response = client.query(**params)

    if 'Item' in response:
      return response['Item']
    else:
      return None

def get_account(account_identifier):
    client = get_dynamo_client()
    filtering_exp = Key('account_identifier').eq(account_identifier)    
    response = client.get_item(
        TableName = ACCOUNT_TABLE,
        Key = {        
            'account_identifier': account_identifier
        }        
    )
    if 'Item' in response:
      return response['Item']
    else:
      return None

def get_batch_write_items(start_num, num_items, accounts):    
    accounts = accounts[start_num:start_num + num_items]    
    for account in accounts:
        count = 0
        for k, v, in list(account.items()):
            count += 1
            account[k] = {'S': str(v)}
            if (count == len(account.items())):
                account['data_created'] = {'S': get_utc_now()}
                account['data_updated'] = {'S': get_utc_now()}

    return accounts

def create_batch_write_structure(table_name, start_num, num_items, accounts):    
    return {
        table_name: [
            {'PutRequest': {'Item': item}}
            for item in get_batch_write_items(start_num, num_items, accounts)
        ]
    }

def save_account(account):
    dynamodb = get_dynamo_client()
    table = dynamodb.Table(ACCOUNT_TABLE)

    table.put_item(
        Item = account
    )

async def save_all_accounts():    
    accounts = reader.get_json_data()
    client = get_dynamo_client()    
    start = 0
    total_records = 0
    while True:
        try:
            request_items = create_batch_write_structure(ACCOUNT_TABLE, start, 25, accounts)
            if request_items[ACCOUNT_TABLE]:
                total_records += len(request_items[ACCOUNT_TABLE])
                response = client.batch_write_item(
                    RequestItems = request_items
                )
                await asyncio.sleep(SLEEP_DURATION_MS)
                if len(response['UnprocessedItems']) == 0:
                    print('.', end='', flush=True)
                    # print('Written "{0}" items to dynamo'.format(total_records))
                else:
                    # Hit the provisioned write limit
                    print('Hit write limit, backing off then retrying')
                    await asyncio.sleep(5)

                    # Items left over that haven't been inserted
                    unprocessed_items = response['UnprocessedItems']
                    print('Resubmitting items')
                    # Loop until unprocessed items are written
                    while len(unprocessed_items) > 0:
                        response = await client.batch_write_item(
                            RequestItems = unprocessed_items
                        )
                        # If any items are still left over, add them to the
                        # list to be written
                        unprocessed_items = response['UnprocessedItems']

                        # If there are items left over, we could do with
                        # sleeping some more
                        if len(unprocessed_items) > 0:
                            print('Backing off for 5 seconds')
                            await asyncio.sleep(5)

                    # Inserted all the unprocessed items, exit loop
                    print('Unprocessed items successfully inserted')
                    break

                start += 25
            else:
                break    
            
        except Exception as e:
            print(f'resource, specify none      : write failed: {e}')
    
    # Complete    
    print('Finished inserting "{0}" records'.format(total_records))
    return total_records
