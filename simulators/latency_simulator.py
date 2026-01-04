import logging
import random
import statistics
import time
from datetime import datetime
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class LatencySimulator:
    
    def __init__(self, 
                 base_latency_ms: float = 100.0,
                 jitter_ms: float = 50.0,
                 spike_probability: float = 0.05,
                 seed: Optional[int] = None):
        if seed:
            random.seed(seed)
        
        self.base_latency_ms = base_latency_ms
        self.jitter_ms = jitter_ms
        self.spike_probability = spike_probability
        
        self.latencies = []
        self.spike_count = 0
        
        logger.info(f"LatencySimulator configured - Base: {base_latency_ms}ms, "
                   f"Jitter: {jitter_ms}ms, Spike prob: {spike_probability*100:.1f}%")
    
    def _calculate_latency(self) -> float:
        latency = random.gauss(self.base_latency_ms, self.jitter_ms / 2)
        
        latency = max(0, latency)
        
        if random.random() < self.spike_probability:
            spike_multiplier = random.uniform(5, 20)
            latency *= spike_multiplier
            self.spike_count += 1
        
        return latency
    
    def wait(self) -> float:
        latency_ms = self._calculate_latency()
        self.latencies.append(latency_ms)
        
        time.sleep(latency_ms / 1000.0)
        
        return latency_ms
    
    def wait_between_events(self) -> Dict[str, float]:
        start_time = time.time()
        latency_ms = self.wait()
        actual_wait = (time.time() - start_time) * 1000
        
        return {
            "target_latency_ms": latency_ms,
            "actual_latency_ms": actual_wait,
            "is_spike": latency_ms > (self.base_latency_ms * 3)
        }
    
    def apply_temporal_pattern(self, hour_of_day: int) -> None:
        if not 0 <= hour_of_day <= 23:
            raise ValueError("hour_of_day must be between 0 and 23")
        
        if 9 <= hour_of_day <= 12 or 14 <= hour_of_day <= 17:
            multiplier = 1.5
        elif 20 <= hour_of_day <= 23:
            multiplier = 1.2
        elif 0 <= hour_of_day <= 6:
            multiplier = 0.5
        else:
            multiplier = 1.0
        
        self.base_latency_ms = self.base_latency_ms * multiplier
    
    def simulate_network_conditions(self, condition: str) -> None:
        conditions = {
            'excellent': {'base': 10, 'jitter': 5, 'spike_prob': 0.001},
            'good': {'base': 50, 'jitter': 20, 'spike_prob': 0.01},
            'normal': {'base': 100, 'jitter': 50, 'spike_prob': 0.05},
            'poor': {'base': 300, 'jitter': 150, 'spike_prob': 0.15},
            'terrible': {'base': 1000, 'jitter': 500, 'spike_prob': 0.30}
        }
        
        if condition not in conditions:
            raise ValueError(f"Condition '{condition}' is not valid. "
                           f"Options: {list(conditions.keys())}")
        
        config = conditions[condition]
        self.base_latency_ms = config['base']
        self.jitter_ms = config['jitter']
        self.spike_probability = config['spike_prob']
        
        logger.info(f"LatencySimulator network condition: {condition.upper()} "
                   f"(base={config['base']}ms, jitter={config['jitter']}ms)")
    
    def get_stats(self) -> Dict:
        if not self.latencies:
            return {
                "count": 0,
                "message": "No latency data yet"
            }
        
        return {
            "count": len(self.latencies),
            "mean_ms": statistics.mean(self.latencies),
            "median_ms": statistics.median(self.latencies),
            "min_ms": min(self.latencies),
            "max_ms": max(self.latencies),
            "stddev_ms": statistics.stdev(self.latencies) if len(self.latencies) > 1 else 0,
            "spike_count": self.spike_count,
            "spike_rate": self.spike_count / len(self.latencies) if self.latencies else 0
        }
    
    def reset_stats(self):
        self.latencies = []
        self.spike_count = 0


class WindowAggregator:
    
    def __init__(self, window_size_seconds: int = 300):
        self.window_size_seconds = window_size_seconds
        self.events = []
        
        logger.info(f"WindowAggregator window configured: {window_size_seconds}s "
                   f"({window_size_seconds/60:.1f} minutes)")
    
    def add_event(self, event: Dict, timestamp: Optional[datetime] = None):
        if timestamp is None:
            timestamp = datetime.now()
        
        self.events.append((timestamp, event))
        self._cleanup_old_events()
    
    def _cleanup_old_events(self):
        now = datetime.now()
        cutoff = now.timestamp() - self.window_size_seconds
        
        self.events = [
            (ts, evt) for ts, evt in self.events 
            if ts.timestamp() >= cutoff
        ]
    
    def get_window_events(self) -> list:
        self._cleanup_old_events()
        return [evt for _, evt in self.events]
    
    def get_window_stats(self) -> Dict:
        self._cleanup_old_events()
        events = [evt for _, evt in self.events]
        
        if not events:
            return {
                "count": 0,
                "window_size_seconds": self.window_size_seconds
            }
        
        high_risk = sum(1 for e in events if e.get('risk', {}).get('risk_level') == 'HIGH')
        low_risk = sum(1 for e in events if e.get('risk', {}).get('risk_level') == 'LOW')
        
        anomalies = sum(1 for e in events if 'anomaly_flags' in e)
        
        credit_limits = [
            e.get('credit', {}).get('credit_limit', 0) 
            for e in events
        ]
        avg_credit_limit = statistics.mean(credit_limits) if credit_limits else 0
        
        return {
            "window_size_seconds": self.window_size_seconds,
            "total_events": len(events),
            "high_risk_events": high_risk,
            "low_risk_events": low_risk,
            "anomalies": anomalies,
            "anomaly_rate": anomalies / len(events) if events else 0,
            "avg_credit_limit": avg_credit_limit,
            "events_per_second": len(events) / self.window_size_seconds
        }
