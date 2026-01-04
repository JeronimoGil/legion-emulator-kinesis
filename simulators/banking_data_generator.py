import logging
import random
from datetime import datetime, timedelta
from typing import Dict, Iterator, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class BankingDataGenerator:
    
    def __init__(self, dataset_path: str, seed: Optional[int] = None):
        if seed:
            random.seed(seed)
        
        self.df = pd.read_excel(dataset_path, header=0)
        self.total_records = len(self.df)
        self.current_index = 0
        
        logger.info(f"BankingDataGenerator dataset loaded: {self.total_records:,} records")
    
    def _map_sex(self, sex_code: int) -> str:
        return "M" if sex_code == 1 else "F"
    
    def _map_education(self, edu_code: int) -> str:
        mapping = {
            1: "GRADUATE_SCHOOL",
            2: "UNIVERSITY", 
            3: "HIGH_SCHOOL",
            4: "OTHERS",
            5: "UNKNOWN",
            6: "UNKNOWN",
            0: "UNKNOWN"
        }
        return mapping.get(edu_code, "UNKNOWN")
    
    def _map_marriage(self, mar_code: int) -> str:
        mapping = {
            1: "MARRIED",
            2: "SINGLE",
            3: "OTHERS",
            0: "UNKNOWN"
        }
        return mapping.get(mar_code, "UNKNOWN")
    
    def _generate_timestamp(self, base_time: Optional[datetime] = None) -> str:
        if base_time is None:
            base_time = datetime.now()
        
        return base_time.isoformat()
    
    def generate_credit_event(self, 
                             event_type: str = "CREDIT_ASSESSMENT",
                             base_time: Optional[datetime] = None) -> Dict:
        row = self.df.iloc[self.current_index % self.total_records]
        self.current_index += 1
        
        event = {
            "event_id": f"EVT-{row['ID']}-{random.randint(1000, 9999)}",
            "event_type": event_type,
            "timestamp": self._generate_timestamp(base_time),
            "source_system": "CREDIT_CARD_SYSTEM",
            
            "customer": {
                "customer_id": f"CUST-{int(row['ID']):06d}",
                "demographic": {
                    "sex": self._map_sex(row['SEX']),
                    "education": self._map_education(row['EDUCATION']),
                    "marital_status": self._map_marriage(row['MARRIAGE']),
                    "age": int(row['AGE'])
                }
            },
            
            "credit": {
                "credit_limit": int(row['LIMIT_BAL']),
                "currency": "TWD"
            },
            
            "payment_history": {
                "september": int(row['PAY_0']),
                "august": int(row['PAY_2']),
                "july": int(row['PAY_3']),
                "june": int(row['PAY_4']),
                "may": int(row['PAY_5']),
                "april": int(row['PAY_6'])
            },
            
            "billing_amounts": {
                "september": int(row['BILL_AMT1']),
                "august": int(row['BILL_AMT2']),
                "july": int(row['BILL_AMT3']),
                "june": int(row['BILL_AMT4']),
                "may": int(row['BILL_AMT5']),
                "april": int(row['BILL_AMT6'])
            },
            
            "payment_amounts": {
                "september": int(row['PAY_AMT1']),
                "august": int(row['PAY_AMT2']),
                "july": int(row['PAY_AMT3']),
                "june": int(row['PAY_AMT4']),
                "may": int(row['PAY_AMT5']),
                "april": int(row['PAY_AMT6'])
            },
            
            "risk": {
                "default_payment_next_month": int(row['default payment next month']),
                "risk_level": "HIGH" if row['default payment next month'] == 1 else "LOW"
            }
        }
        
        return event
    
    def stream_events(self, 
                     count: Optional[int] = None,
                     event_type: str = "CREDIT_ASSESSMENT",
                     start_time: Optional[datetime] = None) -> Iterator[Dict]:
        if count is None:
            count = self.total_records
        
        base_time = start_time or datetime.now()
        
        for i in range(count):
            current_time = base_time + timedelta(seconds=i)
            yield self.generate_credit_event(event_type, current_time)
    
    def reset(self):
        self.current_index = 0
    
    def get_stats(self) -> Dict:
        return {
            "total_records": self.total_records,
            "current_index": self.current_index,
            "default_rate": float(self.df['default payment next month'].mean()),
            "avg_credit_limit": float(self.df['LIMIT_BAL'].mean()),
            "avg_age": float(self.df['AGE'].mean())
        }
