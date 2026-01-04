import json
import logging
import os
import time
import warnings

import boto3
from dotenv import load_dotenv

warnings.filterwarnings('ignore')
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')
os.environ['AWS_DEFAULT_REGION'] = os.getenv('AWS_REGION')

kinesis = boto3.client(
    'kinesis',
    endpoint_url='http://localhost:4566',
    region_name='us-east-1',
    aws_access_key_id='test',
    aws_secret_access_key='test'
)

stream_name = os.getenv('STREAM_NAME')

def get_shard_iterator():
    response = kinesis.describe_stream(StreamName=stream_name)
    shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    
    response = kinesis.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType='TRIM_HORIZON'
    )
    
    return response['ShardIterator']

def consume_records():
    logger.info(f"Starting consumer for stream: {stream_name}")
    
    shard_iterator = get_shard_iterator()
    
    while True:
        response = kinesis.get_records(ShardIterator=shard_iterator, Limit=10)
        
        records = response['Records']
        if records:
            for record in records:
                data = json.loads(record['Data'])
                logger.info(f"Record received: {data}")
        else:
            logger.info("Waiting for new records...")
        
        shard_iterator = response['NextShardIterator']
        
        time.sleep(2)

if __name__ == "__main__":
    consume_records()

