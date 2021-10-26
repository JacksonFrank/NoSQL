# Author: Jackson Frank

import boto3
import csv

S3_BUCKET_NAME = 'jrf114-hw3-data'

def create():
    s3 = boto3.resource('s3', aws_access_key_id=<ACCESS-KEY-HERE>, aws_secret_access_key=<SECRET-ACCESS-KEY-HERE>)

    # create bucket, will cause an exception of bucket is already created
    try:
        s3.create_bucket(Bucket=S3_BUCKET_NAME, CreateBucketConfiguration={'LocationConstraint': 'us-east-2'})
    except Exception as e:
        print(e)
    bucket = s3.Bucket(S3_BUCKET_NAME)

    bucket.Acl().put(ACL='public-read')

    dyndb = boto3.resource('dynamodb', region_name='us-east-2', aws_access_key_id='AKIA5SSKLFNCRZBYFZFX', aws_secret_access_key='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')

    try:
        table = dyndb.create_table(
            TableName='DataTable',
            KeySchema=[
                {
                    'AttributeName': 'PartitionKey',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'RowKey',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'PartitionKey',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'RowKey',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
    except Exception as e:
        print(e)
        table = dyndb.Table('DataTable')

    table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

    with open('experiments.csv', 'r', encoding="utf-8-sig") as maincsvfile:
        maincsvf = csv.DictReader(maincsvfile, delimiter=',', quotechar='|')
        for item in maincsvf:
            print(item)
            body = open(item['FileName'] + ".csv", 'rb')
            s3.Object(S3_BUCKET_NAME, item['FileName']).put(Body=body)
            md = s3.Object(S3_BUCKET_NAME, item['FileName']).Acl().put(ACL='public-read')

            url = "https://" + S3_BUCKET_NAME + ".s3.us-east-2.amazonaws.com/" + item['FileName']
            metadata_item = {
                'PartitionKey': item['Name'],
                'RowKey': item['Id'],
                'description': item['Description'],
                'date': item['Date'],
                'url': url
            }
            try:
                table.put_item(Item=metadata_item)
            except:
                print("Item may already be there or another failure")

def query():
    dyndb = boto3.resource('dynamodb', region_name='us-east-2', aws_access_key_id='AKIA5SSKLFNCRZBYFZFX', aws_secret_access_key='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')
    table = dyndb.Table('DataTable')

    response = table.get_item(
        Key={
            'PartitionKey': 'experiment2',
            'RowKey': '2'
        }
    )

    print("Response:")
    print(response)
    print()
    print("Item:")
    print(response['Item'])


if __name__ == "__main__":
    query()