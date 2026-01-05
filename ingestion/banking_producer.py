import json
import logging
import time

import boto3

from config.settings import AWSConfig, KinesisConfig, ProducerConfig
from simulators.anomaly_injector import AnomalyInjector
from simulators.banking_data_generator import BankingDataGenerator
from simulators.latency_simulator import LatencySimulator, WindowAggregator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BankingEventProducer:
    
    def __init__(self,
                 dataset_path: str,
                 anomaly_rate: float = None,
                 base_latency_ms: float = None,
                 network_condition: str = None):
        
        AWSConfig.set_env_vars()
        
        self.kinesis = boto3.client(
            'kinesis',
            endpoint_url=AWSConfig.LOCALSTACK_ENDPOINT,
            region_name=AWSConfig.REGION,
            aws_access_key_id=AWSConfig.ACCESS_KEY_ID,
            aws_secret_access_key=AWSConfig.SECRET_ACCESS_KEY
        )
        
        self.stream_name = KinesisConfig.STREAM_NAME
        
        anomaly_rate = anomaly_rate or ProducerConfig.ANOMALY_RATE
        base_latency_ms = base_latency_ms or ProducerConfig.BASE_LATENCY_MS
        network_condition = network_condition or ProducerConfig.NETWORK_CONDITION
        
        self.data_generator = BankingDataGenerator(dataset_path)
        self.anomaly_injector = AnomalyInjector(anomaly_rate=anomaly_rate)
        self.latency_simulator = LatencySimulator(base_latency_ms=base_latency_ms)
        self.latency_simulator.simulate_network_conditions(network_condition)
        
        self.window_5min = WindowAggregator(window_size_seconds=300)
        self.window_1hour = WindowAggregator(window_size_seconds=3600)
        
        self.events_sent = 0
        self.errors = 0
        self._start_time = time.time()
        
        logger.info(f"Producer initialized. Target: {self.stream_name}")
    
    def send_to_kinesis(self, event: dict) -> bool:
        response = self.kinesis.put_record(
            StreamName=self.stream_name,
            Data=json.dumps(event, ensure_ascii=False),
            PartitionKey=event['customer']['customer_id']
        )
        
        self.events_sent += 1
        return True
    
    def produce_events(self, count: int = None, show_details: bool = None, max_duration_hours: float = None):
        count = count if count is not None else None
        show_details = show_details if show_details is not None else ProducerConfig.SHOW_DETAILS
        max_duration_hours = max_duration_hours or ProducerConfig.MAX_DURATION_HOURS
        
        if count is None:
            logger.info(f"Starting producer (max {max_duration_hours}h)")
            count = float('inf')
        else:
            logger.info(f"Producing {count} events")
        
        start_time = time.time()
        max_duration_seconds = max_duration_hours * 3600
        i = 0
        
        for base_event in self.data_generator.stream_events(count=None):
            i += 1
            
            elapsed_time = time.time() - start_time
            if count == float('inf') and elapsed_time >= max_duration_seconds:
                logger.info(f"Reached max duration {max_duration_hours}h")
                break
            
            event = self.anomaly_injector.inject(base_event)
            
            self.window_5min.add_event(event)
            self.window_1hour.add_event(event)
            
            success = self.send_to_kinesis(event)
            
            if show_details:
                has_anomaly = 'anomaly_flags' in event
                anomaly_marker = " [ANOMALY]" if has_anomaly else ""
                risk = event['risk']['risk_level']
                customer_id = event['customer']['customer_id']
                
                logger.info(f"Event {i}: {customer_id} | {risk}{anomaly_marker}")
            
            latency_info = self.latency_simulator.wait_between_events()
            if latency_info['is_spike']:
                logger.warning(f"Latency spike: {latency_info['actual_latency_ms']:.0f}ms")
            
            if i % 20 == 0:
                self._show_stats()
            
            if count != float('inf') and i >= count:
                break
        
        self._show_final()
    
    def _show_stats(self):
        stats_5min = self.window_5min.get_window_stats()
        stats_1hour = self.window_1hour.get_window_stats()
        
        logger.info(f"5min: {stats_5min['total_events']} events, {stats_5min['anomalies']} anomalies | "
                   f"1h: {stats_1hour['total_events']} events")
    
    def _show_final(self):
        elapsed = time.time() - self._start_time
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        
        logger.info(f"Sent {self.events_sent} events in {hours}h {minutes}m {seconds}s")
