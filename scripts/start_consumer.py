import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from streaming.consumers.dynamodb_consumer import DynamoDBConsumer

if __name__ == "__main__":
    consumer = DynamoDBConsumer()
    consumer.consume_and_store()

