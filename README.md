# Legion Emulator Kinesis

A realistic banking credit event simulator that generates streaming data from real credit card datasets. This project demonstrates event-driven architecture using AWS Kinesis (via LocalStack), with advanced simulation capabilities including anomaly injection, variable latency patterns, and DynamoDB bronze layer storage.

## Architecture Overview

The application consists of four main components:

- Producer: Generates realistic banking credit events from a real dataset of 30,000 credit card clients and sends them to Kinesis
- Simulators: Independent modules that inject anomalies, simulate network latency, and aggregate events in temporal windows
- Consumer: Reads events from Kinesis stream and stores them in DynamoDB bronze layer
- Bronze Layer: DynamoDB table for raw event storage with indexes for efficient querying

All components use LocalStack to emulate AWS Kinesis and DynamoDB locally without incurring cloud costs.

## Prerequisites

Before running this project, ensure you have the following installed on your system:

- Docker (version 20.10 or higher)
- Docker Compose (version 2.0 or higher)
- Python 3.8 or higher

You can verify your installations by running:

```bash
docker --version
docker-compose --version
python --version
```

## Visualization Dashboard

A web dashboard is available to monitor events in real-time:

```bash
python scripts/start_dashboard.py
```

Open browser: http://localhost:5000

Features:
- Real-time metrics (total events, recent events, high risk, anomalies)
- Event table with filters (Recent, High Risk, Anomalies)
- Auto-refresh every 5 seconds
- Clean and professional UI

## Project Structure

```
legion-emulator-kinesis/
├── config/                          # Configuration management
│   ├── __init__.py
│   └── settings.py                  # Centralized settings (AWS, Kinesis, DynamoDB, etc)
│
├── infrastructure/                  # AWS resource setup
│   ├── __init__.py
│   ├── kinesis_setup.py            # Kinesis stream creation
│   └── dynamodb_setup.py           # DynamoDB table creation
│
├── ingestion/                       # Data producers
│   ├── __init__.py
│   └── banking_producer.py         # Banking event producer with simulators
│
├── streaming/                       # Stream consumers
│   ├── __init__.py
│   └── consumers/
│       ├── __init__.py
│       └── dynamodb_consumer.py    # Consumer that writes to DynamoDB bronze
│
├── analytics/                       # Queries and analysis
│   ├── __init__.py
│   └── bronze_queries.py           # Query engine for bronze layer
│
├── simulators/                      # Event simulation
│   ├── __init__.py
│   ├── data/
│   │   └── credit_card_dataset.xls # Real dataset (30,000 records)
│   ├── banking_data_generator.py   # Generates events from dataset
│   ├── anomaly_injector.py         # Injects synthetic anomalies
│   └── latency_simulator.py        # Simulates network latency
│
├── scripts/                         # Execution scripts
│   ├── __init__.py
│   ├── setup_kinesis.py            # Script to setup Kinesis
│   ├── setup_dynamodb.py           # Script to setup DynamoDB
│   ├── start_producer.py           # Script to start producer
│   ├── start_consumer.py           # Script to start consumer
│   └── query_bronze.py             # Script to query bronze data
│
├── docker-compose.yml               # LocalStack orchestration
├── requirements.txt                 # Python dependencies
└── .env                            # Environment configuration
```

## Dataset Information

This project uses the "Default of Credit Card Clients" dataset from UCI Machine Learning Repository:

- 30,000 credit card client records from Taiwan (2005)
- 23 features including demographics, payment history, billing amounts
- Target variable: probability of default in the next month

Features include:
- Credit limit and demographic information (age, gender, education, marital status)
- 6-month payment history
- 6-month billing and payment amounts
- Default risk labels

## Running the Project

### Automatic Virtual Environment Activation

The project includes `.vscode/settings.json` configuration that automatically activates the virtual environment when opening integrated terminals in VS Code or Cursor. The virtual environment will activate automatically without manual intervention.

For manual activation outside of VS Code/Cursor:

```bash
.\venv\Scripts\Activate.ps1  # Windows PowerShell
# source venv/bin/activate    # Linux/Mac
```

### Complete workflow from start to finish:

Step 1: Activate virtual environment
```bash
# Automatic in VS Code when opening a new terminal
# Manual: .\venv\Scripts\Activate.ps1  # Windows PowerShell
# Manual: source venv/bin/activate     # Linux/Mac
```

Step 2: Install dependencies
```bash
pip install -r requirements.txt
```

Step 3: Start LocalStack
```bash
docker-compose up -d
```
Wait 5 seconds for LocalStack to initialize.

Step 4: Create Kinesis stream
```bash
python scripts/setup_kinesis.py
```
Should show: Stream created, Status: ACTIVE

Step 5: Create DynamoDB table
```bash
python scripts/setup_dynamodb.py
```
Should show: Table created, Status: ACTIVE

Step 6: Start Producer (Terminal 1)
```bash
python scripts/start_producer.py
```
Will show: Event 1: CUST-XXXXXX | HIGH
Continues running indefinitely (max 2 hours)

Step 7: Start Consumer (Terminal 2 - new terminal)
```bash
.\venv\Scripts\Activate.ps1
python scripts/start_consumer.py
```
Will show: Event 1: EVT-XXX-XXXX [JSON data]
Continues running and storing to DynamoDB

Step 8: Start Dashboard (Terminal 3 - new terminal)
```bash
.\venv\Scripts\Activate.ps1
python scripts/start_dashboard.py
```
Open browser: http://localhost:5000
Shows real-time metrics and event table

### Stopping Services

**Stop Producer/Consumer/Dashboard:** Ctrl+C in each terminal
**Stop LocalStack:** `docker-compose down`

### Clean Data (Start Fresh)

To delete all data and start from scratch:
```bash
python scripts/clean_data.py
python scripts/setup_dynamodb.py
```

### Query Data (Command Line)

To query data without dashboard:
```bash
python scripts/query_bronze.py
```

### Step 1: Clone the repository

If you haven't already, navigate to the project directory:

```bash
cd legion-emulator-kinesis
```

## Execution Time Limits

Both the producer and consumer have a **2-hour automatic timeout** to prevent indefinite execution and excessive CPU usage:

### Producer
- Runs continuously for a maximum of 2 hours
- Cycles through the 30,000 record dataset repeatedly
- Shows message when time limit is reached
- Displays final summary with runtime statistics

### Consumer
- Listens for events for a maximum of 2 hours
- Shows remaining time in waiting messages
- Gracefully shuts down at the 2-hour mark
- Reports total records processed

### Stopping Earlier

You can stop either service at any time by pressing `Ctrl+C`. Both services will shut down gracefully and show their final statistics.

### Adjusting Time Limits

To modify the time limits, edit the respective files:

**Producer** (`src/producer.py`):
```python
producer.produce_events(
    count=None,
    show_details=True,
    max_duration_hours=2.0  # Change this value (in hours)
)
```

**Consumer** (`src/consumer.py`):
```python
def consume_records(max_duration_hours: float = 2.0):  # Change this value
```

## Quick Start

Complete setup and execution:

```bash
# 1. Activate virtual environment
.\venv\Scripts\Activate.ps1  # Windows PowerShell

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start LocalStack
docker-compose up -d

# 4. Create infrastructure
python scripts/setup_kinesis.py
python scripts/setup_dynamodb.py

# 5. Start producer (Terminal 1)
python scripts/start_producer.py

# 6. Start consumer (Terminal 2, remember to activate venv)
python scripts/start_consumer.py

# 7. Start dashboard (Terminal 3, remember to activate venv)
python scripts/start_dashboard.py

# 8. Open browser
# http://localhost:5000
```

## Clean Data

To delete all data and start fresh:

```bash
python scripts/clean_data.py
python scripts/setup_dynamodb.py
```

## Configuration

### Producer Configuration

You can customize the producer behavior by editing configuration in `config/settings.py`:

```python
class ProducerConfig:
    ANOMALY_RATE = 0.08              # Adjust anomaly injection rate (0.0 to 1.0)
    BASE_LATENCY_MS = 150             # Base latency between events in milliseconds
    NETWORK_CONDITION = 'good'        # Network condition: 'excellent', 'good', 'poor', 'terrible'
    MAX_DURATION_HOURS = 2.0          # Maximum runtime in hours
    SHOW_DETAILS = True               # Show detailed logs for each event
```

### Anomaly Types

The simulator can inject six types of anomalies:

1. **Unusual Credit Limit**: Extremely high, low, or negative credit limits
2. **Payment Pattern Anomalies**: Consistent severe payment delays across all months
3. **Billing Mismatches**: Excessive overpayments or consistent non-payment
4. **Demographic Inconsistencies**: Invalid age or education inconsistent with age
5. **Duplicate Events**: Potential duplicate transactions
6. **Missing Fields**: Critical fields missing from event data

### Network Conditions

Pre-configured network simulation profiles:

- `excellent`: 10ms base, minimal jitter, 0.1% spike probability
- `good`: 50ms base, moderate jitter, 1% spike probability
- `normal`: 100ms base, standard jitter, 5% spike probability
- `poor`: 300ms base, high jitter, 15% spike probability
- `terrible`: 1000ms base, extreme jitter, 30% spike probability

### Environment Variables

Create a `.env` file in the project root with the following variables:

```bash
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_REGION=us-east-1
LOCALSTACK_ENDPOINT=http://localhost:4566
STREAM_NAME=local-kinesis-stream
SHARD_COUNT=1
```

## Example Output

### Normal Event

```json
{
  "event_id": "EVT-1-7486",
  "event_type": "CREDIT_ASSESSMENT",
  "timestamp": "2026-01-04T17:14:28.514482",
  "source_system": "CREDIT_CARD_SYSTEM",
  "customer": {
    "customer_id": "CUST-000001",
    "demographic": {
      "sex": "F",
      "education": "UNIVERSITY",
      "marital_status": "MARRIED",
      "age": 24
    }
  },
  "credit": {
    "credit_limit": 20000,
    "currency": "TWD"
  },
  "payment_history": {
    "september": 2,
    "august": 2,
    "july": -1,
    "june": -1,
    "may": -2,
    "april": -2
  },
  "risk": {
    "default_payment_next_month": 1,
    "risk_level": "HIGH"
  }
}
```

### Event with Anomaly

```json
{
  "event_id": "EVT-12-3421",
  "event_type": "CREDIT_ASSESSMENT",
  "anomaly_flags": [
    {
      "type": "INVALID_AGE",
      "severity": "MEDIUM",
      "description": "Age 5 is outside valid range"
    }
  ],
  "customer": { "..." },
  "credit": { "..." },
  "risk": { "..." }
}
```

## Monitoring and Logs

All components use Python's logging module with consistent formatting:

```
%(asctime)s - %(name)s - %(levelname)s - %(message)s
```

The producer displays:
- Individual event processing status
- Anomaly detection markers
- Latency spike warnings
- Temporal window statistics (every 20 events)
- Time remaining when running in infinite mode
- Final summary with aggregated metrics and total runtime

The consumer displays:
- Each received event with full details
- Time remaining when waiting for records
- Total records processed and runtime at shutdown

To view LocalStack logs:

```bash
docker-compose logs -f
```

## Stopping the Services

### Manual Stop

To stop the producer or consumer, press `Ctrl+C` in their respective terminals. Both services will:
- Catch the interrupt signal gracefully
- Display final summary statistics
- Report total runtime and records processed

### Automatic Stop

Both services automatically stop after **2 hours** of execution to prevent indefinite CPU consumption:
- Producer: Shows final summary with total events sent and runtime
- Consumer: Shows total records processed and runtime

To stop LocalStack:

```bash
docker-compose down
```

This stops and removes the LocalStack container while preserving the Docker image.

## Troubleshooting

### LocalStack fails to start

Check if port 4566 is already in use:

```bash
netstat -an | findstr 4566
```

If another service is using the port, stop it or modify the port mapping in `docker-compose.yml`.

### Cannot load dataset

Verify the dataset file exists at `simulators/data/credit_card_dataset.xls`. The file should be included in the repository.

Ensure you have the required dependencies installed:

```bash
pip install pandas openpyxl xlrd
```

### Consumer not receiving events

- Verify LocalStack is running: `docker ps`
- Verify the stream exists: `python scripts/setup_kinesis.py`
- Ensure the producer is running and sending events
- Check that both producer and consumer use the same stream name

### Encoding errors on Windows

If you encounter encoding issues with Unicode characters, the simulators include UTF-8 configuration for Windows. If problems persist, run this in your terminal before executing scripts:

```bash
chcp 65001
```

## Development Notes

The project follows these standards:

- Professional data engineering folder structure
- Separation of concerns (config, infrastructure, ingestion, streaming, analytics)
- Centralized configuration management
- Modular architecture with clear interfaces
- All code comments and documentation in English
- Logging module used for operational logs
- Type hints used throughout for better code documentation

Project organization:
- `config/`: Centralized configuration for all components
- `infrastructure/`: AWS resource setup and provisioning
- `ingestion/`: Data producers and event generators
- `streaming/`: Stream consumers and processors
- `analytics/`: Query engines and data analysis
- `simulators/`: Event simulation modules
- `scripts/`: Entry points for executing different components

To modify simulation behavior without changing the dataset:
- Adjust anomaly injection rates in the producer
- Change network conditions to simulate different scenarios
- Modify window sizes for temporal aggregations

## Clean Up

To completely remove all containers and volumes:

```bash
docker-compose down -v
```

Note that this will delete any data stored in LocalStack.

## Use Cases

This project is ideal for:

1. **Learning Stream Processing**: Experiment with Kinesis without AWS costs
2. **Anomaly Detection**: Train ML models with realistic synthetic anomalies
3. **System Testing**: Simulate variable loads and network conditions
4. **Credit Risk Analysis**: Process banking events in real-time
5. **Event-Driven Architecture**: Practice with producer-consumer patterns

## References

- **Dataset**: [UCI ML Repository - Default of Credit Card Clients](https://archive.ics.uci.edu/ml/datasets/default+of+credit+card+clients)
- **Paper**: Yeh, I. C., & Lien, C. H. (2009). The comparisons of data mining techniques for the predictive accuracy of probability of default of credit card clients. Expert Systems with Applications, 36(2), 2473-2480.
- **LocalStack**: https://localstack.cloud/
- **AWS Kinesis**: https://aws.amazon.com/kinesis/

## Data Privacy

The dataset used is public and anonymized. All data is used exclusively for educational and research purposes.
