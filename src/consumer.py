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

def consume_records(max_duration_hours: float = 2.0):
    logger.info(f"Starting consumer for stream: {stream_name}")
    logger.info(f"Maximum runtime: {max_duration_hours} hours")
    
    start_time = time.time()
    max_duration_seconds = max_duration_hours * 3600
    shard_iterator = get_shard_iterator()
    records_processed = 0
    
    try:
        while True:
            # Check if maximum runtime has been reached
            elapsed_time = time.time() - start_time
            if elapsed_time >= max_duration_seconds:
                logger.info(f"\n\nReached maximum duration of {max_duration_hours} hours. Stopping consumer...")
                break
            
            response = kinesis.get_records(ShardIterator=shard_iterator, Limit=10)
            
            records = response['Records']
            if records:
                # Process each record from the stream
                for record in records:
                    data = json.loads(record['Data'])
                    logger.info(f"Record received: {data}")
                    records_processed += 1
            else:
                # Display remaining time when idle
                remaining_seconds = max_duration_seconds - elapsed_time
                remaining_minutes = int(remaining_seconds // 60)
                logger.info(f"Waiting for new records... (Time remaining: {remaining_minutes}m)")
            
            shard_iterator = response['NextShardIterator']
            
            time.sleep(2)
    
    except KeyboardInterrupt:
        logger.info("\n\nConsumer stopped by user")
    finally:
        elapsed = time.time() - start_time
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        logger.info(f"\nTotal records processed: {records_processed}")
        logger.info(f"Total runtime: {hours}h {minutes}m {seconds}s")
        logger.info("Consumer shutdown complete.")

if __name__ == "__main__":
    consume_records()
