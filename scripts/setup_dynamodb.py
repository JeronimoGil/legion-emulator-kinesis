import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from infrastructure.dynamodb_setup import DynamoDBSetup

if __name__ == "__main__":
    setup = DynamoDBSetup()
    setup.setup()

