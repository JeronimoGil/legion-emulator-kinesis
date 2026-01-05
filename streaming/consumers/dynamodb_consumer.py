import json
import logging
import time

import boto3
from boto3.dynamodb.types import TypeSerializer

from config.settings import AWSConfig, KinesisConfig, DynamoDBConfig, ConsumerConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DynamoDBWriter:
    
    def __init__(self):
        AWSConfig.set_env_vars()
        
        self.dynamodb = boto3.client(
            'dynamodb',
            endpoint_url=AWSConfig.LOCALSTACK_ENDPOINT,
            region_name=AWSConfig.REGION,
            aws_access_key_id=AWSConfig.ACCESS_KEY_ID,
            aws_secret_access_key=AWSConfig.SECRET_ACCESS_KEY
        )
        
        self.table_name = DynamoDBConfig.BRONZE_TABLE_NAME
        self.serializer = TypeSerializer()
        self.records_written = 0
        self.write_errors = 0
    
    def serialize_event(self, event: dict) -> dict:
        dynamodb_item = {}
        
        for key, value in event.items():
            if value is not None:
                dynamodb_item[key] = self.serializer.serialize(value)
        
        dynamodb_item['customer_id'] = self.serializer.serialize(
            event['customer']['customer_id']
        )
        dynamodb_item['risk_level'] = self.serializer.serialize(
            event['risk']['risk_level']
        )
        
        return dynamodb_item
    
    def write_to_dynamodb(self, event: dict) -> bool:
        dynamodb_item = self.serialize_event(event)
        
        response = self.dynamodb.put_item(
            TableName=self.table_name,
            Item=dynamodb_item
        )
        
        self.records_written += 1
        return True
    
    def get_stats(self) -> dict:
        return {
            'records_written': self.records_written,
            'write_errors': self.write_errors
        }


class DynamoDBConsumer:
    
    def __init__(self):
        AWSConfig.set_env_vars()
        
        self.kinesis = boto3.client(
            'kinesis',
            endpoint_url=AWSConfig.LOCALSTACK_ENDPOINT,
            region_name=AWSConfig.REGION,
            aws_access_key_id=AWSConfig.ACCESS_KEY_ID,
            aws_secret_access_key=AWSConfig.SECRET_ACCESS_KEY
        )
        
        self.stream_name = KinesisConfig.STREAM_NAME
        self.db_writer = DynamoDBWriter()
    
    def get_shard_iterator(self):
        response = self.kinesis.describe_stream(StreamName=self.stream_name)
        shard_id = response['StreamDescription']['Shards'][0]['ShardId']
        
        response = self.kinesis.get_shard_iterator(
            StreamName=self.stream_name,
            ShardId=shard_id,
            ShardIteratorType='TRIM_HORIZON'
        )
        
        return response['ShardIterator']
    
    def consume_and_store(self, max_duration_hours: float = None):
        max_duration_hours = max_duration_hours or ConsumerConfig.MAX_DURATION_HOURS
        
        logger.info(f"Starting consumer. Target: {self.db_writer.table_name} (max {max_duration_hours}h)")
        
        start_time = time.time()
        max_duration_seconds = max_duration_hours * 3600
        shard_iterator = self.get_shard_iterator()
        
        records_processed = 0
        
        while True:
            elapsed_time = time.time() - start_time
            if elapsed_time >= max_duration_seconds:
                logger.info(f"Reached max duration {max_duration_hours}h")
                break
            
            response = self.kinesis.get_records(
                ShardIterator=shard_iterator,
                Limit=ConsumerConfig.BATCH_SIZE
            )
            
            records = response['Records']
            if records:
                for record in records:
                    event_data = json.loads(record['Data'])
                    
                    self.db_writer.write_to_dynamodb(event_data)
                    records_processed += 1
                    
                    has_anomaly = 'anomaly_flags' in event_data
                    anomaly_marker = " [ANOMALY]" if has_anomaly else ""
                    
                    logger.info(f"Event {records_processed}: {event_data['event_id']}{anomaly_marker}")
                    logger.info(json.dumps(event_data, indent=2, ensure_ascii=False))
                    
                    if records_processed % ConsumerConfig.STATS_INTERVAL == 0:
                        stats = self.db_writer.get_stats()
                        logger.info(f"Written: {stats['records_written']} records")
            else:
                remaining_minutes = int((max_duration_seconds - elapsed_time) // 60)
                logger.info(f"Waiting for records ({remaining_minutes}m remaining)")
            
            shard_iterator = response['NextShardIterator']
            time.sleep(ConsumerConfig.POLL_INTERVAL_SECONDS)
        
        elapsed = time.time() - start_time
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        
        stats = self.db_writer.get_stats()
        logger.info(f"Processed {records_processed} records in {hours}h {minutes}m {seconds}s")

