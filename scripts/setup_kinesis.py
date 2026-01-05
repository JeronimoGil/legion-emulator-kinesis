import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from infrastructure.kinesis_setup import KinesisSetup

if __name__ == "__main__":
    setup = KinesisSetup()
    setup.setup()

