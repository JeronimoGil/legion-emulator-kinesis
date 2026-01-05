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
    return {
        'event_id': event['event_id'],
        'timestamp': event['timestamp'],
        'customer_id': event['customer']['customer_id'],
        'risk_level': event['risk']['risk_level'],
        'credit_limit': event['credit']['credit_limit'],
        'has_anomaly': 'anomaly_flags' in event,
        'anomaly_types': [f['type'] for f in event.get('anomaly_flags', [])]
    }


if __name__ == '__main__':
    app.run(debug=True, port=5000)

