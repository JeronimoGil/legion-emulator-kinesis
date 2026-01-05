import logging
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import boto3
from config.settings import AWSConfig, DynamoDBConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def clean_dynamodb():
    AWSConfig.set_env_vars()
    
    dynamodb = boto3.client(
        'dynamodb',
        endpoint_url=AWSConfig.LOCALSTACK_ENDPOINT,
        region_name=AWSConfig.REGION,
        aws_access_key_id=AWSConfig.ACCESS_KEY_ID,
        aws_secret_access_key=AWSConfig.SECRET_ACCESS_KEY
    )
    
    table_name = DynamoDBConfig.BRONZE_TABLE_NAME
    
    logger.info(f"Deleting table {table_name}")
    
    response = dynamodb.list_tables()
    if table_name not in response['TableNames']:
        logger.info("Table does not exist")
        return
    
    dynamodb.delete_table(TableName=table_name)
    logger.info("Table deleted successfully")
    logger.info("Run 'python scripts/setup_dynamodb.py' to recreate")


if __name__ == "__main__":
    clean_dynamodb()

