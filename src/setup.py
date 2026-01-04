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
shard_count = int(os.getenv('SHARD_COUNT', 1))

def wait_for_localstack():
    logger.info("Waiting for LocalStack to start...")
    time.sleep(5)

def create_stream():
    logger.info(f"Creating Kinesis stream: {stream_name}")
    
    kinesis.create_stream(
        StreamName=stream_name,
        ShardCount=shard_count
    )
    
    logger.info("Stream created successfully")

def verify_stream():
    logger.info("Verifying stream...")
    
    response = kinesis.describe_stream(StreamName=stream_name)
    
    stream_info = response['StreamDescription']
    logger.info(f"Stream Name: {stream_info['StreamName']}")
    logger.info(f"Status: {stream_info['StreamStatus']}")
    logger.info(f"Shards: {len(stream_info['Shards'])}")
    logger.info(f"ARN: {stream_info['StreamARN']}")

if __name__ == "__main__":
    logger.info("Configuring LocalStack for Kinesis...")
    
    wait_for_localstack()
    create_stream()
    verify_stream()
    
    logger.info("Configuration completed!")

