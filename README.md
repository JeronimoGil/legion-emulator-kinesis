# LocalStack + Kinesis Project

Isolated project to experiment with Amazon Kinesis using LocalStack.

## Why LocalStack for Kinesis?

LocalStack provides a high-fidelity simulation of Amazon Kinesis that runs entirely on your local machine. This makes it ideal for development and learning without incurring AWS costs or requiring internet connectivity.

**What makes it a good simulation:**

- **API Compatibility**: LocalStack implements the official AWS Kinesis API, meaning the same `boto3` code that works locally will work in production AWS without modifications.
- **Core Features**: Supports essential Kinesis operations including stream creation, shard management, `put_record`, `get_records`, and shard iterators.
- **Realistic Behavior**: Maintains data ordering within shards, respects partition keys for data distribution, and implements proper sequence numbers.
- **Persistence**: Data persists across container restarts when configured, simulating the 24-hour retention period behavior of real Kinesis.
- **Development Workflow**: Enables rapid iteration and testing of producer/consumer patterns without the latency and cost of cloud deployments.

**What it doesn't simulate**: Advanced features like enhanced fan-out, data retention beyond restarts (in simplified mode), and some CloudWatch metrics. For learning core Kinesis concepts and building basic streaming applications, LocalStack provides more than adequate fidelity.

## Requirements

- Docker
- Docker Compose
- Python 3.8+
- AWS CLI (optional, can use Python setup script instead)

## Quick Start

1. Create and activate a Python virtual environment (recommended):

Linux/Mac:
```bash
python -m venv venv
source venv/bin/activate
```

Windows PowerShell:
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

Windows CMD:
```cmd
python -m venv venv
venv\Scripts\activate.bat
```

2. Install Python dependencies:

```bash
pip install -r requirements.txt
```

3. Start LocalStack:

```bash
docker-compose up -d
```

4. Configure the stream:

Option A - Using Python (recommended, works on all platforms):
```bash
python scripts/setup.py
```

Option B - Using shell scripts (requires AWS CLI):

Linux/Mac
```bash
bash scripts/setup.sh
```

Windows PowerShell
```powershell
.\scripts\setup.ps1
```

5. Run the producer:

```bash
python scripts/producer.py
```

6. Run the consumer (in another terminal):

```bash
python scripts/consumer.py
```

## Useful Commands

View LocalStack logs:
```bash
docker-compose logs -f
```

Stop LocalStack:
```bash
docker-compose down
```

Stop and remove data:
```bash
docker-compose down -v
```

List Kinesis streams:
```bash
aws kinesis list-streams --endpoint-url http://localhost:4566 --region us-east-1
```

Describe stream:
```bash
aws kinesis describe-stream --stream-name local-kinesis-stream --endpoint-url http://localhost:4566 --region us-east-1
```

## Project Structure

```
.
├── docker-compose.yml     # LocalStack configuration
├── localstack-data/       # Persistent data (gitignored)
├── scripts/
│   ├── setup.py          # Initial setup script (Python, recommended)
│   ├── setup.sh          # Initial setup script (Linux/Mac, requires AWS CLI)
│   ├── setup.ps1         # Initial setup script (Windows, requires AWS CLI)
│   ├── producer.py       # Producer example
│   └── consumer.py       # Consumer example
├── .env                   # Environment variables
├── .gitignore
├── requirements.txt       # Python dependencies
└── README.md
```

## Configuration

Environment variables are defined in `.env`:

- LOCALSTACK_ENDPOINT: LocalStack endpoint URL
- AWS_REGION: AWS region to use
- AWS_ACCESS_KEY_ID: AWS access key (test value for local)
- AWS_SECRET_ACCESS_KEY: AWS secret key (test value for local)
- STREAM_NAME: Kinesis stream name
- SHARD_COUNT: Number of shards

## Troubleshooting

If LocalStack fails to start, verify Docker is running:
```bash
docker --version
docker ps
```

If the stream creation fails, wait a few more seconds for LocalStack to fully initialize.

If AWS CLI is not recognized in PowerShell:
- Close all PowerShell windows and open a new one
- Or use the Python setup script: `python scripts/setup.py`

If you get execution policy errors in PowerShell:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

To deactivate the virtual environment when finished:
```bash
deactivate
```

To reset everything:
```bash
docker-compose down -v
docker-compose up -d
```

## Testing

To verify everything works:

1. Start LocalStack: `docker-compose up -d`
2. Run setup script: `python scripts/setup.py`
3. Run producer: `python scripts/producer.py` (sends 10 test messages)
4. Run consumer: `python scripts/consumer.py` (press Ctrl+C to stop)

You should see the consumer receiving all messages sent by the producer in real-time.

