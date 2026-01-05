import logging
from datetime import datetime, timedelta

import boto3
from boto3.dynamodb.types import TypeDeserializer

from config.settings import AWSConfig, DynamoDBConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BronzeQueryEngine:
    
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
        self.deserializer = TypeDeserializer()
    
    def deserialize_item(self, item: dict) -> dict:
        python_item = {}
        for key, value in item.items():
            python_item[key] = self.deserializer.deserialize(value)
        return python_item
    
    def get_event_by_id(self, event_id: str):
        logger.info(f"Querying event by ID: {event_id}")
        
        response = self.dynamodb.query(
            TableName=self.table_name,
            KeyConditionExpression='event_id = :event_id',
            ExpressionAttributeValues={
                ':event_id': {'S': event_id}
            },
            Limit=10
        )
        
        items = [self.deserialize_item(item) for item in response['Items']]
        
        logger.info(f"Found {len(items)} events with ID {event_id}")
        return items
    
    def get_events_by_customer(self, customer_id: str, limit: int = 10):
        logger.info(f"Querying events for customer: {customer_id}")
        
        response = self.dynamodb.query(
            TableName=self.table_name,
            IndexName='customer_id-timestamp-index',
            KeyConditionExpression='customer_id = :customer_id',
            ExpressionAttributeValues={
                ':customer_id': {'S': customer_id}
            },
            Limit=limit,
            ScanIndexForward=False
        )
        
        items = [self.deserialize_item(item) for item in response['Items']]
        
        logger.info(f"Found {len(items)} events for customer {customer_id}")
        return items
    
    def get_high_risk_events(self, limit: int = 10):
        logger.info(f"Querying high risk events")
        
        response = self.dynamodb.query(
            TableName=self.table_name,
            IndexName='risk_level-timestamp-index',
            KeyConditionExpression='risk_level = :risk',
            ExpressionAttributeValues={
                ':risk': {'S': 'HIGH'}
            },
            Limit=limit,
            ScanIndexForward=False
        )
        
        items = [self.deserialize_item(item) for item in response['Items']]
        
        logger.info(f"Found {len(items)} high risk events")
        return items
    
    def get_anomaly_events(self, limit: int = 20):
        logger.info(f"Scanning for anomaly events")
        
        response = self.dynamodb.scan(
            TableName=self.table_name,
            FilterExpression='attribute_exists(anomaly_flags)',
            Limit=limit
        )
        
        items = [self.deserialize_item(item) for item in response['Items']]
        
        logger.info(f"Found {len(items)} events with anomalies")
        return items
    
    def count_total_events(self) -> int:
        logger.info("Counting total events in table")
        
        response = self.dynamodb.scan(
            TableName=self.table_name,
            Select='COUNT'
        )
        
        count = response['Count']
        logger.info(f"Total events in bronze table: {count}")
        return count
    
    def get_recent_events(self, minutes: int = 5, limit: int = 20):
        logger.info(f"Querying events from last {minutes} minutes")
        
        cutoff_time = (datetime.now() - timedelta(minutes=minutes)).isoformat()
        
        response = self.dynamodb.scan(
            TableName=self.table_name,
            FilterExpression='#ts > :cutoff_time',
            ExpressionAttributeNames={
                '#ts': 'timestamp'
            },
            ExpressionAttributeValues={
                ':cutoff_time': {'S': cutoff_time}
            },
            Limit=limit
        )
        
        items = [self.deserialize_item(item) for item in response['Items']]
        
        logger.info(f"Found {len(items)} events in last {minutes} minutes")
        return items


def display_event(event: dict):
    logger.info(f"Event: {event['event_id']} | Customer: {event['customer']['customer_id']} | Risk: {event['risk']['risk_level']}")
    if 'anomaly_flags' in event:
        for flag in event['anomaly_flags']:
            logger.info(f"  Anomaly: {flag['type']} ({flag['severity']})")


def main():
    logger.info("Querying bronze layer")
    
    query_engine = BronzeQueryEngine()
    
    total_count = query_engine.count_total_events()
    
    if total_count == 0:
        logger.info("No events found. Run producer and consumer first.")
        return
    
    logger.info(f"\n1. Recent events ({total_count} total)")
    recent_events = query_engine.get_recent_events(minutes=10, limit=5)
    for event in recent_events:
        display_event(event)
    
    logger.info("\n2. High risk events")
    high_risk_events = query_engine.get_high_risk_events(limit=5)
    for event in high_risk_events:
        display_event(event)
    
    logger.info("\n3. Events with anomalies")
    anomaly_events = query_engine.get_anomaly_events(limit=5)
    for event in anomaly_events:
        display_event(event)
    
    if recent_events:
        sample_customer_id = recent_events[0]['customer']['customer_id']
        logger.info(f"\n4. Events for customer {sample_customer_id}")
        customer_events = query_engine.get_events_by_customer(sample_customer_id, limit=5)
        for event in customer_events:
            display_event(event)


if __name__ == "__main__":
    main()

