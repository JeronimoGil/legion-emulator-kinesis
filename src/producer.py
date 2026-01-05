import json
import logging
import os
import sys
import time

import boto3
from dotenv import load_dotenv

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

from simulators.anomaly_injector import AnomalyInjector
from simulators.banking_data_generator import BankingDataGenerator
from simulators.latency_simulator import LatencySimulator, WindowAggregator

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')
os.environ['AWS_DEFAULT_REGION'] = os.getenv('AWS_REGION')

kinesis = boto3.client(
    'kinesis',
    endpoint_url=os.getenv('LOCALSTACK_ENDPOINT', 'http://localhost:4566'),
    region_name=os.getenv('AWS_REGION', 'us-east-1'),
    aws_access_key_id='test',
    aws_secret_access_key='test'
)

stream_name = os.getenv('STREAM_NAME', 'local-kinesis-stream')


class BankingEventProducer:
    
    def __init__(self,
                 dataset_path: str,
                 anomaly_rate: float = 0.05,
                 base_latency_ms: float = 100,
                 network_condition: str = 'good'):
        logger.info("="*80)
        logger.info("INITIALIZING BANKING EVENT PRODUCER")
        logger.info("="*80)
        
        self.data_generator = BankingDataGenerator(dataset_path)
        self.anomaly_injector = AnomalyInjector(anomaly_rate=anomaly_rate)
        self.latency_simulator = LatencySimulator(base_latency_ms=base_latency_ms)
        self.latency_simulator.simulate_network_conditions(network_condition)
        
        self.window_5min = WindowAggregator(window_size_seconds=300)
        self.window_1hour = WindowAggregator(window_size_seconds=3600)
        
        self.events_sent = 0
        self.errors = 0
        self._start_time = time.time()
        
        logger.info(f"Target stream: {stream_name}")
        logger.info("="*80)
    
    def send_to_kinesis(self, event: dict) -> bool:
        try:
            response = kinesis.put_record(
                StreamName=stream_name,
                Data=json.dumps(event, ensure_ascii=False),
                PartitionKey=event['customer']['customer_id']
            )
            
            self.events_sent += 1
            return True
            
        except Exception as e:
            logger.error(f"Error sending event to Kinesis: {e}")
            self.errors += 1
            return False
    
    def produce_events(self, count: int = 100, show_details: bool = True, max_duration_hours: float = 2.0):
        if count is None:
            logger.info(f"Starting continuous event production (max {max_duration_hours}h, press Ctrl+C to stop early)...")
            count = float('inf')
        else:
            logger.info(f"Starting production of {count} events...")
        
        logger.info("")
        
        start_time = time.time()
        max_duration_seconds = max_duration_hours * 3600
        i = 0
        
        try:
            for base_event in self.data_generator.stream_events(count=None):
                i += 1
                
                # Check time limit for infinite mode
                elapsed_time = time.time() - start_time
                if count == float('inf') and elapsed_time >= max_duration_seconds:
                    logger.info(f"\n\nReached maximum duration of {max_duration_hours} hours. Stopping gracefully...")
                    break
                
                event = self.anomaly_injector.inject(base_event)
                
                self.window_5min.add_event(event)
                self.window_1hour.add_event(event)
                
                success = self.send_to_kinesis(event)
                
                if show_details:
                    has_anomaly = 'anomaly_flags' in event
                    anomaly_marker = " [ANOMALY!]" if has_anomaly else ""
                    risk = event['risk']['risk_level']
                    customer_id = event['customer']['customer_id']
                    
                    if count == float('inf'):
                        logger.info(
                            f"Event {i}: {customer_id} | "
                            f"Risk: {risk} | "
                            f"Status: {'OK' if success else 'ERROR'}{anomaly_marker}"
                        )
                    else:
                        logger.info(
                            f"Event {i}/{count}: {customer_id} | "
                            f"Risk: {risk} | "
                            f"Status: {'OK' if success else 'ERROR'}{anomaly_marker}"
                        )
                    
                    if has_anomaly and show_details:
                        for flag in event['anomaly_flags']:
                            logger.info(f"  └─ [{flag['severity']}] {flag['type']}")
                
                latency_info = self.latency_simulator.wait_between_events()
                if latency_info['is_spike']:
                    logger.warning(f"  └─ Latency spike: {latency_info['actual_latency_ms']:.1f}ms")
                
                # Show statistics every 20 events
                if i % 20 == 0:
                    self._show_window_stats()
                
                # Stop when reaching target count in finite mode
                if count != float('inf') and i >= count:
                    break
                    
        except KeyboardInterrupt:
            logger.info("\n\nProduction stopped by user")
        finally:
            self._show_final_summary()
    
    def _show_window_stats(self):
        logger.info("")
        logger.info("-" * 80)
        logger.info("TEMPORAL WINDOW STATISTICS")
        logger.info("-" * 80)
        
        stats_5min = self.window_5min.get_window_stats()
        logger.info(f"5-minute window: {stats_5min['total_events']} events | "
                   f"Anomalies: {stats_5min['anomalies']} ({stats_5min['anomaly_rate']*100:.1f}%) | "
                   f"High Risk: {stats_5min['high_risk_events']}")
        
        stats_1hour = self.window_1hour.get_window_stats()
        logger.info(f"1-hour window: {stats_1hour['total_events']} events | "
                   f"Anomalies: {stats_1hour['anomalies']} ({stats_1hour['anomaly_rate']*100:.1f}%) | "
                   f"High Risk: {stats_1hour['high_risk_events']}")
        
        logger.info("-" * 80)
        logger.info("")
    
    def _show_final_summary(self):
        logger.info("")
        logger.info("="*80)
        logger.info("FINAL SUMMARY")
        logger.info("="*80)
        
        gen_stats = self.data_generator.get_stats()
        logger.info(f"Events generated: {gen_stats['current_index']}")
        logger.info(f"Dataset default rate: {gen_stats['default_rate']*100:.2f}%")
        
        anom_stats = self.anomaly_injector.get_stats()
        logger.info(f"Anomalies injected: {anom_stats['anomalies_injected']} "
                   f"({anom_stats['actual_anomaly_rate']*100:.1f}%)")
        
        lat_stats = self.latency_simulator.get_stats()
        logger.info(f"Average latency: {lat_stats['mean_ms']:.1f}ms "
                   f"(min: {lat_stats['min_ms']:.1f}ms, max: {lat_stats['max_ms']:.1f}ms)")
        logger.info(f"Latency spikes: {lat_stats['spike_count']} "
                   f"({lat_stats['spike_rate']*100:.1f}%)")
        
        logger.info(f"Events sent to Kinesis: {self.events_sent}")
        logger.info(f"Errors: {self.errors}")
        
        if hasattr(self, '_start_time'):
            elapsed = time.time() - self._start_time
            hours = int(elapsed // 3600)
            minutes = int((elapsed % 3600) // 60)
            seconds = int(elapsed % 60)
            logger.info(f"Total runtime: {hours}h {minutes}m {seconds}s")
        
        logger.info("="*80)


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    dataset_path = os.path.join(
        project_root, 
        "simulators",
        "data",
        "credit_card_dataset.xls"
    )
    
    producer = BankingEventProducer(
        dataset_path=dataset_path,
        anomaly_rate=0.08,
        base_latency_ms=150,
        network_condition='good'
    )
    
    producer.produce_events(
        count=None,
        show_details=True,
        max_duration_hours=2.0
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n\nProducer stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
