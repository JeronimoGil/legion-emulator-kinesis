import os
import sys
from datetime import datetime

from flask import Flask, render_template, jsonify

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from analytics.bronze_queries import BronzeQueryEngine

app = Flask(__name__)
query_engine = BronzeQueryEngine()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/stats')
def get_stats():
    total = query_engine.count_total_events()
    recent = query_engine.get_recent_events(minutes=5, limit=100)
    
    high_risk_count = sum(1 for e in recent if e.get('risk', {}).get('risk_level') == 'HIGH')
    anomaly_count = sum(1 for e in recent if 'anomaly_flags' in e)
    
    return jsonify({
        'total_events': total,
        'recent_events': len(recent),
        'high_risk': high_risk_count,
        'anomalies': anomaly_count
    })


@app.route('/api/events/recent')
def get_recent_events():
    events = query_engine.get_recent_events(minutes=10, limit=50)
    return jsonify([format_event(e) for e in events])


@app.route('/api/events/high_risk')
def get_high_risk():
    events = query_engine.get_high_risk_events(limit=50)
    return jsonify([format_event(e) for e in events])


@app.route('/api/events/anomalies')
def get_anomalies():
    events = query_engine.get_anomaly_events(limit=50)
    return jsonify([format_event(e) for e in events])


def format_event(event):
    demographic = event.get('customer', {}).get('demographic', {})
    payment_history = event.get('payment_history', {})
    billing = event.get('billing', {})
    payments = event.get('payments', {})
    risk = event.get('risk', {})
    
    return {
        'event_id': event['event_id'],
        'timestamp': event['timestamp'],
        'event_type': event.get('event_type', 'N/A'),
        'source_system': event.get('source_system', 'N/A'),
        'customer_id': event['customer']['customer_id'],
        'sex': demographic.get('sex', 'N/A'),
        'age': demographic.get('age', 'N/A'),
        'education': demographic.get('education', 'N/A'),
        'marital_status': demographic.get('marital_status', 'N/A'),
        'credit_limit': event.get('credit', {}).get('credit_limit', 0),
        'currency': event.get('credit', {}).get('currency', 'N/A'),
        'pay_september': payment_history.get('september', 'N/A'),
        'pay_august': payment_history.get('august', 'N/A'),
        'pay_july': payment_history.get('july', 'N/A'),
        'pay_june': payment_history.get('june', 'N/A'),
        'pay_may': payment_history.get('may', 'N/A'),
        'pay_april': payment_history.get('april', 'N/A'),
        'bill_september': billing.get('bill_amt_september', 'N/A'),
        'bill_august': billing.get('bill_amt_august', 'N/A'),
        'bill_july': billing.get('bill_amt_july', 'N/A'),
        'bill_june': billing.get('bill_amt_june', 'N/A'),
        'bill_may': billing.get('bill_amt_may', 'N/A'),
        'bill_april': billing.get('bill_amt_april', 'N/A'),
        'payment_september': payments.get('payment_amt_september', 'N/A'),
        'payment_august': payments.get('payment_amt_august', 'N/A'),
        'payment_july': payments.get('payment_amt_july', 'N/A'),
        'payment_june': payments.get('payment_amt_june', 'N/A'),
        'payment_may': payments.get('payment_amt_may', 'N/A'),
        'payment_april': payments.get('payment_amt_april', 'N/A'),
        'risk_level': risk.get('risk_level', 'N/A'),
        'default_payment': risk.get('default_payment_next_month', 'N/A'),
        'has_anomaly': 'anomaly_flags' in event,
        'anomaly_count': len(event.get('anomaly_flags', [])),
        'anomaly_types': ', '.join([f['type'] for f in event.get('anomaly_flags', [])]) if 'anomaly_flags' in event else '',
        'anomaly_severities': ', '.join([f['severity'] for f in event.get('anomaly_flags', [])]) if 'anomaly_flags' in event else ''
    }


if __name__ == '__main__':
    app.run(debug=True, port=5000)

