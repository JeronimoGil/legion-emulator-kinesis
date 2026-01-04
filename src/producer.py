import json
import logging
import os
import sys

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
    """
    Producer that integrates all simulation modules
    to generate realistic banking events.
    """
    
    def __init__(self,
                 dataset_path: str,
                 anomaly_rate: float = 0.05,
                 base_latency_ms: float = 100,
                 network_condition: str = 'good'):
        """
        Initialize producer with all simulators.
        
        Args:
            dataset_path: Path to credit card dataset
            anomaly_rate: Anomaly injection rate (0.0 to 1.0)
            base_latency_ms: Base latency between events
            network_condition: Network condition ('excellent', 'good', 'poor', 'terrible')
        """
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
        
        logger.info(f"Target stream: {stream_name}")
        logger.info("="*80)
    
    def send_to_kinesis(self, event: dict) -> bool:
        """
        Send an event to Kinesis.
        
        Args:
            event: Banking event to send
        
        Returns:
            True if sent successfully, False otherwise
        """
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
    
    def produce_events(self, count: int = 100, show_details: bool = True):
        """
        Generate and send events to Kinesis.
        
        Args:
            count: Number of events to generate
            show_details: Whether to show details for each event
        """
        logger.info(f"Starting production of {count} events...")
        logger.info("")
        
        for i, base_event in enumerate(self.data_generator.stream_events(count=count), 1):
            event = self.anomaly_injector.inject(base_event)
            
            self.window_5min.add_event(event)
            self.window_1hour.add_event(event)
            
            success = self.send_to_kinesis(event)
            
            if show_details:
                has_anomaly = 'anomaly_flags' in event
                anomaly_marker = " [ANOMALY!]" if has_anomaly else ""
                risk = event['risk']['risk_level']
                customer_id = event['customer']['customer_id']
                
                logger.info(
                    f"Event {i}/{count}: {customer_id} | "
                    f"Risk: {risk} | "
                    f"Status: {'OK' if success else 'ERROR'}{anomaly_marker}"
                )
                
                if has_anomaly and show_details:
                    for flag in event['anomaly_flags']:
                        logger.info(f"  └─ [{flag['severity']}] {flag['type']}")
            
            if i < count:
                latency_info = self.latency_simulator.wait_between_events()
                if latency_info['is_spike']:
                    logger.warning(f"  └─ Latency spike: {latency_info['actual_latency_ms']:.1f}ms")
            
            if i % 20 == 0:
                self._show_window_stats()
        
        self._show_final_summary()
    
    def _show_window_stats(self):
        """Display temporal window statistics."""
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
        """Display final production summary."""
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
        
        logger.info("="*80)


def main():
    """Main producer function."""
    
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
        count=50,
        show_details=True
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n\nProducer stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)

