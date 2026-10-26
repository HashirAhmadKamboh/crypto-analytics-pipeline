# Crypto Analytics Pipeline

This repository implements a local, real-time crypto data pipeline for **BTCUSDT**:

1. `ingestion/producer.py` streams trades from Binance WebSocket.
2. Trades are published to Kafka topic `crypto-ticks`.
3. `processing/spark_processor.py` reads Kafka with PySpark Structured Streaming, transforms records, and appends them to PostgreSQL.
4. `backend/main.py` serves historical REST data and near-real-time WebSocket updates from PostgreSQL.

> **Important:** A frontend/dashboard is **not** included in this repository.

## End-to-end data flow

```mermaid
flowchart LR
    A[Binance WebSocket\nbtcusdt@trade] --> B[Python Kafka Producer\ningestion/producer.py]
    B --> C[Kafka topic\ncrypto-ticks]
    C --> D[PySpark Structured Streaming\nprocessing/spark_processor.py]
    D --> E[(PostgreSQL\ncryptodb.crypto_ticks)]
    E --> F[FastAPI backend\nbackend/main.py]
    F --> G[REST: /api/historical/{symbol}]
    F --> H[WebSocket: /ws/live/{symbol}]
```

## Repository structure

```text
.
├── README.md
├── docker-compose.yml
├── pyrightconfig.json
├── crypto-analytics-pipeline.code-workspace
├── ingestion/
│   └── producer.py
├── processing/
│   ├── spark_processor.py
│   ├── postgresql-42.6.0.jar
│   └── spark_checkpoints/            # runtime checkpoint directory used by Spark
└── backend/
    ├── main.py
    ├── database.py
    ├── models.py
    ├── init_db.py
    ├── schemas.py                    # currently empty
    ├── postgresql-42.6.0.jar
    ├── pyrightconfig.json
    └── pyrefly.toml
```

## Prerequisites

- Docker + Docker Compose
- Python 3 environment for running scripts
- Java runtime compatible with PySpark
- A local PostgreSQL JDBC driver jar is already present at:
  - `processing/postgresql-42.6.0.jar`

## Infrastructure (Kafka + PostgreSQL)

From repository root:

```bash
docker compose up -d
```

This starts:

- Zookeeper (`confluentinc/cp-zookeeper:7.4.0`) on `2181`
- Kafka (`confluentinc/cp-kafka:7.4.0`) on `9092`
- PostgreSQL (`postgres:15-alpine`) on `5432`
  - DB: `cryptodb`
  - User: `admin`
  - Password: `adminpassword`

## Python dependencies

There is currently **no** `requirements.txt` or `pyproject.toml` in this repository.
Install dependencies in your Python environment separately.

Required runtime packages (from code imports):

- `fastapi`
- `uvicorn`
- `sqlalchemy`
- `asyncpg`
- `kafka-python`
- `websocket-client`
- `pyspark`

## Database initialization

Create tables before running the API/streaming stack:

```bash
cd /home/runner/work/crypto-analytics-pipeline/crypto-analytics-pipeline/backend
python init_db.py
```

This creates table `crypto_ticks` defined in `backend/models.py`.

## Run each service

### 1) Ingestion producer (Binance -> Kafka)

```bash
cd /home/runner/work/crypto-analytics-pipeline/crypto-analytics-pipeline/ingestion
python producer.py
```

Behavior:
- Connects to `wss://stream.binance.com:9443/ws/btcusdt@trade`
- Maps Binance payload to:
  - `symbol`
  - `price`
  - `quantity`
  - `timestamp` (epoch milliseconds)
- Publishes JSON to Kafka topic `crypto-ticks` at `localhost:9092`
- Retries every 2 seconds while Kafka is unavailable

### 2) Spark processor (Kafka -> PostgreSQL)

```bash
cd /home/runner/work/crypto-analytics-pipeline/crypto-analytics-pipeline/processing
python spark_processor.py
```

Processing details from code:

- Derives Kafka connector package dynamically from installed PySpark version:
  - `org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}`
- Reads Kafka topic `crypto-ticks` from `localhost:9092`
- Uses `startingOffsets = latest`
- Parses JSON schema:
  - `symbol: string`
  - `price: double`
  - `quantity: double`
  - `timestamp: long`
- Transforms:
  - `price = floor(price * 100000) / 100000`
  - `quantity = floor(quantity * 100000) / 100000`
  - `timestamp = to_timestamp(timestamp / 1000)` (epoch ms -> SQL timestamp)
- Writes with `foreachBatch` JDBC append to PostgreSQL table `crypto_ticks`
- Uses checkpoint location: `./spark_checkpoints`

### 3) FastAPI backend

Because backend files use local imports (`from database import ...`), either run from `backend/` or set `PYTHONPATH` accordingly.

Option A (from backend directory):

```bash
cd /home/runner/work/crypto-analytics-pipeline/crypto-analytics-pipeline/backend
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Option B (from repo root with `PYTHONPATH`):

```bash
cd /home/runner/work/crypto-analytics-pipeline/crypto-analytics-pipeline
PYTHONPATH=backend uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## API reference

Base URL (local): `http://localhost:8000`

### GET `/api/historical/{symbol}`

Returns up to `limit` most-recent rows, then reverses in-memory so response order is **oldest -> newest**.

Example request:

```bash
curl "http://localhost:8000/api/historical/BTCUSDT?limit=5"
```

Representative response shape:

```json
[
  {
    "id": 101,
    "symbol": "BTCUSDT",
    "price": 67123.12345,
    "quantity": 0.01234,
    "timestamp": "2026-10-26T10:10:10"
  }
]
```

### WebSocket `/ws/live/{symbol}`

- Polls PostgreSQL once per second for latest row of the requested symbol
- Sends message only when a new row id appears

Example connect URL:

```text
ws://localhost:8000/ws/live/BTCUSDT
```

Representative WebSocket message:

```json
{
  "id": 102,
  "symbol": "BTCUSDT",
  "price": 67123.54321,
  "quantity": 0.03456,
  "timestamp": "2026-10-26T10:10:11.123456"
}
```

## Hard-coded configuration (current code)

- Binance stream URL: `wss://stream.binance.com:9443/ws/btcusdt@trade`
- Kafka broker: `localhost:9092`
- Kafka topic: `crypto-ticks`
- PostgreSQL DSN (backend): `postgresql+asyncpg://<user>:<password>@localhost:5432/cryptodb` (hard-coded as user `admin` and password `adminpassword`)
- PostgreSQL JDBC URL (Spark): `jdbc:postgresql://localhost:5432/cryptodb`
- PostgreSQL table: `crypto_ticks`
- API service port (when run as above): `8000`
- Docker service ports:
  - Zookeeper `2181`
  - Kafka `9092`
  - PostgreSQL `5432`

> Security note: database credentials are hard-coded for local development and should be externalized (env vars/secrets) before non-local deployment.

## Operational notes / troubleshooting

- Start order matters: bring up Docker services before producer/Spark/API.
- Producer retries Kafka connection automatically; if Kafka is still starting, wait.
- Run `backend/init_db.py` once before expecting API/Spark writes to succeed.
- Spark requires:
  - working Java runtime
  - ability to resolve the Spark Kafka connector package matching installed PySpark
  - access to local JDBC jar (`processing/postgresql-42.6.0.jar`)
- Spark checkpoints are persisted in `processing/spark_checkpoints/`.
- No frontend code exists in this repository; only backend/data pipeline components are present.

## Current limitations / future improvements

Grounded in current codebase state:

- Only `BTCUSDT` is ingested (single hard-coded Binance stream).
- Core runtime config is hard-coded (URLs, topic, credentials, ports).
- No dependency lockfile/manifest in repository.
- No automated tests in repository.
- No DB migration framework; table creation is direct metadata create.
- No authentication/authorization on API or WebSocket endpoints.
- No frontend/dashboard implementation in this repository.
