import logging
import time

import boto3

from config.settings import AWSConfig, KinesisConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class KinesisSetup:
    
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
        self.shard_count = KinesisConfig.SHARD_COUNT
    
    def wait_for_localstack(self):
        time.sleep(5)
    
    def stream_exists(self) -> bool:
        response = self.kinesis.list_streams()
        return self.stream_name in response['StreamNames']
    
    def create_stream(self):
        if self.stream_exists():
            logger.info(f"Stream {self.stream_name} already exists")
            return
        
        logger.info(f"Creating stream {self.stream_name}")
        
        self.kinesis.create_stream(
            StreamName=self.stream_name,
            ShardCount=self.shard_count
        )
        
        logger.info("Stream created")
    
    def verify_stream(self):
        response = self.kinesis.describe_stream(StreamName=self.stream_name)
        stream_info = response['StreamDescription']
        
        logger.info(f"Stream: {stream_info['StreamName']}, Status: {stream_info['StreamStatus']}, Shards: {len(stream_info['Shards'])}")
    
    def setup(self):
        self.wait_for_localstack()
        self.create_stream()
        self.verify_stream()


def main():
    setup = KinesisSetup()
    setup.setup()


if __name__ == "__main__":
    main()

