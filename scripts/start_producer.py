import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config.settings import PathConfig
from ingestion.banking_producer import BankingEventProducer

if __name__ == "__main__":
    dataset_path = PathConfig.get_dataset_path()
    
    producer = BankingEventProducer(dataset_path=dataset_path)
    producer.produce_events()

