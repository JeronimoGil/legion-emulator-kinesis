import boto3
import json
import time
from datetime import datetime
from dotenv import load_dotenv
import os
import warnings
import logging

warnings.filterwarnings('ignore')
load_dotenv()

# Configurar logger simple
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

def send_record(data):
    response = kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey=str(data.get('id', 'default'))
    )
    logger.info(f"Record sent: {data}")
    return response

if __name__ == "__main__":
    logger.info(f"Starting producer for stream: {stream_name}")
    
    for i in range(10):
        data = {
            'id': i,
            'timestamp': datetime.now().isoformat(),
            'message': f'Test message #{i}',
            'value': i * 10
        }
        
        send_record(data)
        time.sleep(1)
    
    logger.info("Producer finished")

