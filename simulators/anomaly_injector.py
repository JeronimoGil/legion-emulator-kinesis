import copy
import logging
import random
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class AnomalyInjector:
    
    def __init__(self, anomaly_rate: float = 0.05, seed: Optional[int] = None):
        if not 0.0 <= anomaly_rate <= 1.0:
            raise ValueError("anomaly_rate must be between 0.0 and 1.0")
        
        self.anomaly_rate = anomaly_rate
        if seed:
            random.seed(seed)
        
        self.anomaly_count = 0
        self.total_processed = 0
        
        logger.info(f"AnomalyInjector configured with anomaly rate: {anomaly_rate*100:.1f}%")
    
    def _should_inject_anomaly(self) -> bool:
        return random.random() < self.anomaly_rate
    
    def _inject_unusual_credit_limit(self, event: Dict) -> Dict:
        event_copy = copy.deepcopy(event)
        
        choice = random.choice(['extremely_high', 'extremely_low', 'negative'])
        
        if choice == 'extremely_high':
            event_copy['credit']['credit_limit'] = random.randint(5_000_000, 10_000_000)
            event_copy['anomaly_flags'] = event_copy.get('anomaly_flags', [])
            event_copy['anomaly_flags'].append({
                "type": "UNUSUAL_CREDIT_LIMIT_HIGH",
                "severity": "HIGH",
                "description": "Credit limit exceeds normal range by 10x"
            })
        
        elif choice == 'extremely_low':
            event_copy['credit']['credit_limit'] = random.randint(100, 1000)
            event_copy['anomaly_flags'] = event_copy.get('anomaly_flags', [])
            event_copy['anomaly_flags'].append({
                "type": "UNUSUAL_CREDIT_LIMIT_LOW",
                "severity": "MEDIUM",
                "description": "Credit limit below minimum threshold"
            })
        
        else:
            event_copy['credit']['credit_limit'] = random.randint(-100000, -1000)
            event_copy['anomaly_flags'] = event_copy.get('anomaly_flags', [])
            event_copy['anomaly_flags'].append({
                "type": "INVALID_CREDIT_LIMIT",
                "severity": "CRITICAL",
                "description": "Negative credit limit detected"
            })
        
        return event_copy
    
    def _inject_payment_pattern_anomaly(self, event: Dict) -> Dict:
        event_copy = copy.deepcopy(event)
        
        for month in event_copy['payment_history'].keys():
            event_copy['payment_history'][month] = random.randint(5, 9)
        
        event_copy['anomaly_flags'] = event_copy.get('anomaly_flags', [])
        event_copy['anomaly_flags'].append({
            "type": "SEVERE_PAYMENT_DELAYS",
            "severity": "HIGH",
            "description": "Consistent payment delays across all months"
        })
        
        return event_copy
    
    def _inject_billing_mismatch(self, event: Dict) -> Dict:
        event_copy = copy.deepcopy(event)
        
        choice = random.choice(['overpayment', 'underpayment'])
        
        if choice == 'overpayment':
            for month in event_copy['billing_amounts'].keys():
                bill = event_copy['billing_amounts'][month]
                if bill > 0:
                    event_copy['payment_amounts'][month] = bill * random.randint(5, 20)
            
            event_copy['anomaly_flags'] = event_copy.get('anomaly_flags', [])
            event_copy['anomaly_flags'].append({
                "type": "EXCESSIVE_OVERPAYMENT",
                "severity": "HIGH",
                "description": "Payment amounts significantly exceed billing amounts"
            })
        
        else:
            for month in event_copy['billing_amounts'].keys():
                event_copy['billing_amounts'][month] = random.randint(50000, 200000)
                event_copy['payment_amounts'][month] = 0
            
            event_copy['anomaly_flags'] = event_copy.get('anomaly_flags', [])
            event_copy['anomaly_flags'].append({
                "type": "CONSISTENT_NON_PAYMENT",
                "severity": "CRITICAL",
                "description": "High billing with zero payments across all months"
            })
        
        return event_copy
    
    def _inject_demographic_inconsistency(self, event: Dict) -> Dict:
        event_copy = copy.deepcopy(event)
        
        choice = random.choice(['impossible_age', 'inconsistent_education'])
        
        if choice == 'impossible_age':
            event_copy['customer']['demographic']['age'] = random.choice(
                [random.randint(5, 15), random.randint(120, 200)]
            )
            
            event_copy['anomaly_flags'] = event_copy.get('anomaly_flags', [])
            event_copy['anomaly_flags'].append({
                "type": "INVALID_AGE",
                "severity": "MEDIUM",
                "description": f"Age {event_copy['customer']['demographic']['age']} is outside valid range"
            })
        
        else:
            event_copy['customer']['demographic']['age'] = random.randint(12, 16)
            event_copy['customer']['demographic']['education'] = "GRADUATE_SCHOOL"
            
            event_copy['anomaly_flags'] = event_copy.get('anomaly_flags', [])
            event_copy['anomaly_flags'].append({
                "type": "DEMOGRAPHIC_INCONSISTENCY",
                "severity": "MEDIUM",
                "description": "Graduate school education inconsistent with age"
            })
        
        return event_copy
    
    def _inject_duplicate_event(self, event: Dict) -> Dict:
        event_copy = copy.deepcopy(event)
        
        event_copy['is_duplicate'] = True
        event_copy['anomaly_flags'] = event_copy.get('anomaly_flags', [])
        event_copy['anomaly_flags'].append({
            "type": "DUPLICATE_EVENT",
            "severity": "LOW",
            "description": "Potential duplicate event detected"
        })
        
        return event_copy
    
    def _inject_missing_fields(self, event: Dict) -> Dict:
        event_copy = copy.deepcopy(event)
        
        if 'payment_amounts' in event_copy:
            del event_copy['payment_amounts']
        
        if 'demographic' in event_copy.get('customer', {}):
            event_copy['customer']['demographic']['age'] = None
        
        event_copy['anomaly_flags'] = event_copy.get('anomaly_flags', [])
        event_copy['anomaly_flags'].append({
            "type": "MISSING_CRITICAL_FIELDS",
            "severity": "HIGH",
            "description": "One or more critical fields are missing"
        })
        
        return event_copy
    
    def inject(self, event: Dict) -> Dict:
        self.total_processed += 1
        
        if not self._should_inject_anomaly():
            return event
        
        anomaly_types = [
            self._inject_unusual_credit_limit,
            self._inject_payment_pattern_anomaly,
            self._inject_billing_mismatch,
            self._inject_demographic_inconsistency,
            self._inject_duplicate_event,
            self._inject_missing_fields
        ]
        
        anomaly_func = random.choice(anomaly_types)
        anomalous_event = anomaly_func(event)
        
        self.anomaly_count += 1
        
        return anomalous_event
    
    def get_stats(self) -> Dict:
        return {
            "total_processed": self.total_processed,
            "anomalies_injected": self.anomaly_count,
            "actual_anomaly_rate": (
                self.anomaly_count / self.total_processed 
                if self.total_processed > 0 else 0.0
            ),
            "configured_rate": self.anomaly_rate
        }
