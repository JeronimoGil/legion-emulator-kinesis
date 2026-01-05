import os
from dotenv import load_dotenv

load_dotenv()


class AWSConfig:
    ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'test')
    SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'test')
    REGION = os.getenv('AWS_REGION', 'us-east-1')
    LOCALSTACK_ENDPOINT = os.getenv('LOCALSTACK_ENDPOINT', 'http://localhost:4566')
    
    @classmethod
    def set_env_vars(cls):
        os.environ['AWS_ACCESS_KEY_ID'] = cls.ACCESS_KEY_ID
        os.environ['AWS_SECRET_ACCESS_KEY'] = cls.SECRET_ACCESS_KEY
        os.environ['AWS_DEFAULT_REGION'] = cls.REGION


class KinesisConfig:
    STREAM_NAME = os.getenv('STREAM_NAME', 'local-kinesis-stream')
    SHARD_COUNT = int(os.getenv('SHARD_COUNT', '1'))


class DynamoDBConfig:
    BRONZE_TABLE_NAME = 'banking_events_bronze'
    READ_CAPACITY_UNITS = 10
    WRITE_CAPACITY_UNITS = 10
    GSI_READ_CAPACITY_UNITS = 5
    GSI_WRITE_CAPACITY_UNITS = 5


class ProducerConfig:
    ANOMALY_RATE = 0.08
    BASE_LATENCY_MS = 150
    NETWORK_CONDITION = 'good'
    MAX_DURATION_HOURS = 2.0
    SHOW_DETAILS = True


class ConsumerConfig:
    MAX_DURATION_HOURS = 2.0
    BATCH_SIZE = 10
    POLL_INTERVAL_SECONDS = 2
    STATS_INTERVAL = 20


class LoggingConfig:
    LEVEL = 'INFO'
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'


class PathConfig:
    @staticmethod
    def get_project_root():
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    @staticmethod
    def get_dataset_path():
        root = PathConfig.get_project_root()
        return os.path.join(root, 'simulators', 'data', 'credit_card_dataset.xls')

