import logging
import time

import boto3

from config.settings import AWSConfig, DynamoDBConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DynamoDBSetup:
    
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
    
    def wait_for_localstack(self):
        response = self.dynamodb.list_tables()
        return True
    
    def table_exists(self) -> bool:
        response = self.dynamodb.list_tables()
        return self.table_name in response['TableNames']
    
    def create_bronze_table(self):
        if self.table_exists():
            logger.info(f"Table {self.table_name} already exists")
            return
        
        logger.info(f"Creating table {self.table_name}")
        
        self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[
                {'AttributeName': 'event_id', 'KeyType': 'HASH'},
                {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'event_id', 'AttributeType': 'S'},
                {'AttributeName': 'timestamp', 'AttributeType': 'S'},
                {'AttributeName': 'customer_id', 'AttributeType': 'S'},
                {'AttributeName': 'risk_level', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'customer_id-timestamp-index',
                    'KeySchema': [
                        {'AttributeName': 'customer_id', 'KeyType': 'HASH'},
                        {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': DynamoDBConfig.GSI_READ_CAPACITY_UNITS,
                        'WriteCapacityUnits': DynamoDBConfig.GSI_WRITE_CAPACITY_UNITS
                    }
                },
                {
                    'IndexName': 'risk_level-timestamp-index',
                    'KeySchema': [
                        {'AttributeName': 'risk_level', 'KeyType': 'HASH'},
                        {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': DynamoDBConfig.GSI_READ_CAPACITY_UNITS,
                        'WriteCapacityUnits': DynamoDBConfig.GSI_WRITE_CAPACITY_UNITS
                    }
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': DynamoDBConfig.READ_CAPACITY_UNITS,
                'WriteCapacityUnits': DynamoDBConfig.WRITE_CAPACITY_UNITS
            }
        )
        
        logger.info("Table created")
    
    def describe_table(self):
        response = self.dynamodb.describe_table(TableName=self.table_name)
        table_info = response['Table']
        
        logger.info(f"Table: {table_info['TableName']}, Status: {table_info['TableStatus']}")
        
        if 'GlobalSecondaryIndexes' in table_info:
            gsi_names = [gsi['IndexName'] for gsi in table_info['GlobalSecondaryIndexes']]
            logger.info(f"GSI: {', '.join(gsi_names)}")
    
    def setup(self):
        self.wait_for_localstack()
        self.create_bronze_table()
        time.sleep(2)
        self.describe_table()


def main():
    setup = DynamoDBSetup()
    setup.setup()


if __name__ == "__main__":
    main()

