# ZeroGEX - Options Analytics Platform

**Real-time gamma exposure (GEX) calculations for SPY/SPX options using TradeStation API integration.**

ZeroGEX is a sophisticated options trading platform that calculates real-time gamma exposure for equity options, providing traders with critical market positioning data. The platform integrates with TradeStation's API for live market data and historical backfilling capabilities.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Main Ingestion Engine](#main-ingestion-engine)
  - [TradeStation Client](#tradestation-client)
  - [Backfill Manager](#backfill-manager)
  - [Stream Manager](#stream-manager)
- [Database Schema](#database-schema)
- [Development](#development)
- [Project Structure](#project-structure)
- [Environment Variables](#environment-variables)
- [Monitoring & Observability](#monitoring--observability)
- [Troubleshooting](#troubleshooting)
- [Roadmap](#roadmap)
- [License](#license)

---

## Features

### ✅ Core Capabilities (Production Ready)
- **Real-time Options Data Streaming** - Live underlying quotes and options chain data with intelligent polling
- **Historical Data Backfilling** - Configurable lookback periods with automatic gap detection
- **1-Minute Data Aggregation** - OHLC aggregation for underlying and last/bid/ask for options
- **Intelligent Strike Selection** - Automatically tracks strikes near current price with dynamic recalculation
- **Multi-Expiration Support** - Configurable number of expiration dates to track
- **Market Hours Detection** - Dynamic polling intervals based on market session (regular/extended/closed)
- **Robust Error Handling** - Exponential backoff retry logic for API failures
- **Data Validation** - Comprehensive validation of all API responses
- **Database Storage** - PostgreSQL/TimescaleDB with proper timezone handling
- **Memory Management** - Automatic cleanup of expired strikes to prevent memory leaks
- **Graceful Shutdown** - Proper buffer flushing and connection cleanup on shutdown

### 🚀 Future Enhancements (Foundation Ready)
- **WebSocket Streaming** - Low-latency real-time data (config ready)
- **Real-time GEX Calculation** - Gamma exposure engine (schema ready)
- **Data Quality Monitoring** - Automated detection of stale/missing data (schema ready)
- **Grafana Dashboards** - Real-time visualization and alerting (metrics schema ready)
- **Greeks Calculation** - Delta, gamma, theta, vega (schema ready)

### 📊 TradeStation API Integration
- Quote snapshots (equity & options)
- Historical OHLCV bars (1min, 5min, daily, etc.)
- Options chain data (expirations, strikes, quotes)
- Symbol search and metadata
- Market depth (Level 2)
- Retry logic with exponential backoff
- Configurable timeouts and batch sizes

---

## Architecture

```
ZeroGEX Platform
├── Data Ingestion Layer
│   ├── TradeStation API Client (with retry logic)
│   ├── Backfill Manager (fetches historical data)
│   └── Stream Manager (fetches real-time data)
├── Main Ingestion Engine
│   ├── Orchestrates backfill → streaming
│   ├── 1-minute data aggregation
│   ├── Buffer management and flushing
│   └── Database storage coordination
├── Data Storage Layer
│   ├── PostgreSQL (relational data)
│   ├── TimescaleDB (time-series optimization)
│   └── Helper views for delta calculations
├── Validation & Configuration
│   ├── Data validation utilities
│   ├── Timezone handling (pytz)
│   └── Centralized configuration
└── Future Layers
    ├── GEX Calculation Engine
    ├── Data Quality Monitor
    ├── WebSocket Stream Manager
    └── Metrics & Monitoring (Prometheus/Grafana)
```

### Data Flow

```
TradeStation API
      ↓
BackfillManager / StreamManager (fetch & yield data)
      ↓
MainEngine (aggregate in 1-minute buckets)
      ↓
PostgreSQL/TimescaleDB (store with timezone awareness)
      ↓
Views (calculate deltas with LAG() functions)
```

---

## Prerequisites

### Required
- **Python 3.8+** (tested on 3.10)
- **PostgreSQL 12+** (for data persistence)
- **TradeStation API Account** - [Sign up here](https://api.tradestation.com/docs/)
  - Client ID
  - Client Secret
  - Refresh Token

### Optional
- **TimescaleDB** (for time-series optimization)
- **AWS Account** (for Secrets Manager password storage)
- **Grafana** (for visualization dashboards)
- **Prometheus** (for metrics collection)

---

## Installation

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd zerogex-oa
```

### 2. Set Up Python Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # Linux/Mac
# OR
venv\Scripts\activate     # Windows
```

### 3. Install Dependencies

```bash
# Install the package in editable mode with all dependencies
pip install -e .

# Or install with development dependencies
pip install -e ".[dev]"
```

**Core dependencies:**
- `requests` - HTTP client for API calls
- `python-dotenv` - Environment variable management
- `psycopg2-binary` - PostgreSQL adapter
- `pandas` - Data manipulation
- `websocket-client` - WebSocket support (future)
- `boto3` - AWS SDK (for Secrets Manager)
- `pytz` - Timezone handling

### 4. Set Up Database

```bash
# Create database
createdb zerogex

# Run schema migration
psql -d zerogex -f sql/001_create_tables.sql
```

**For TimescaleDB (recommended):**

```bash
# Install TimescaleDB extension
psql -d zerogex -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"

# Convert tables to hypertables
psql -d zerogex -c "SELECT create_hypertable('underlying_quotes', 'timestamp', if_not_exists => TRUE);"
psql -d zerogex -c "SELECT create_hypertable('option_chains', 'timestamp', if_not_exists => TRUE);"
```

---

## Configuration

### 1. Create Environment File

```bash
cp .env.example .env
```

### 2. Configure TradeStation API Credentials

Edit `.env` and add your credentials:

```bash
# Required: TradeStation API Credentials
TRADESTATION_CLIENT_ID=your_client_id_here
TRADESTATION_CLIENT_SECRET=your_client_secret_here
TRADESTATION_REFRESH_TOKEN=your_refresh_token_here
```

### 3. Configure Database Connection

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=zerogex
DB_USER=postgres

# For local development
DB_PASSWORD_PROVIDER=env
DB_PASSWORD=your_password_here

# For production (AWS Secrets Manager)
DB_PASSWORD_PROVIDER=aws_secrets_manager
DB_SECRET_NAME=zerogex/db/password
AWS_REGION=us-east-1
```

### 4. Get TradeStation API Credentials

1. Go to [TradeStation Developer Portal](https://api.tradestation.com/docs/)
2. Create a new application
3. Copy your **Client ID** and **Client Secret**
4. Generate a **Refresh Token**:

```bash
python scripts/get_tradestation_tokens.py
```

### 5. Review Configuration Options

See [Environment Variables](#environment-variables) section for complete configuration options.

---

## Usage

### Main Ingestion Engine

**The primary entry point for the complete data pipeline:**

```bash
# Run with defaults (7 days backfill, then stream)
python -m src.ingestion.main_engine

# Custom configuration
python -m src.ingestion.main_engine \
    --underlying SPY \
    --lookback-days 14 \
    --expirations 5 \
    --strike-distance 20.0

# Debug mode
python -m src.ingestion.main_engine --debug

# Get help
python -m src.ingestion.main_engine --help
```

**What it does:**
1. **Backfill Phase** - Fetches historical data for configured lookback period
2. **Gap Detection** - Checks for missing data and backfills gaps (if enabled)
3. **Streaming Phase** - Streams real-time data with dynamic polling
4. **1-Minute Aggregation** - Buffers and aggregates data into 1-minute bars
5. **Database Storage** - Stores aggregated data in PostgreSQL

**Production deployment:**

```bash
# Create systemd service
sudo nano /etc/systemd/system/zerogex-ingest.service
```

```ini
[Unit]
Description=ZeroGEX Ingestion Engine
After=network.target postgresql.service

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/zerogex-oa
Environment="PATH=/home/ubuntu/zerogex-oa/venv/bin"
ExecStart=/home/ubuntu/zerogex-oa/venv/bin/python -m src.ingestion.main_engine
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start service
sudo systemctl enable zerogex-ingest
sudo systemctl start zerogex-ingest

# Check status
sudo systemctl status zerogex-ingest

# View logs
journalctl -u zerogex-ingest -f
```

---

### TradeStation Client

**Test TradeStation API connection and endpoints:**

```bash
# Run all tests
python -m src.ingestion.tradestation_client

# Test specific endpoint
python -m src.ingestion.tradestation_client --test quote --symbol SPY
python -m src.ingestion.tradestation_client --test bars --symbol SPY --bars-back 10
python -m src.ingestion.tradestation_client --test options --symbol SPY

# Enable debug logging
python -m src.ingestion.tradestation_client --debug

# Multiple symbols
python -m src.ingestion.tradestation_client --test quote --symbol SPY,QQQ,AAPL
```

**Available tests:**
- `quote` - Current quotes
- `bars` - Historical OHLCV bars
- `options` - Option expirations and strikes
- `search` - Symbol search
- `market-hours` - Market status and hours
- `depth` - Level 2 market depth

---

### Backfill Manager

**Standalone historical data backfill (for testing/manual use):**

```bash
# Backfill last 1 day of 5-minute data
python -m src.ingestion.backfill_manager

# Backfill last 3 days
python -m src.ingestion.backfill_manager --lookback-days 3

# Daily bars for 30 days
python -m src.ingestion.backfill_manager --unit Daily --lookback-days 30

# Sample options every 10 bars (faster)
python -m src.ingestion.backfill_manager --sample-every 10
```

**Note:** In production, use `main_engine` which handles backfill automatically.

---

### Stream Manager

**Standalone real-time streaming (for testing/manual use):**

```bash
# Stream SPY with defaults
python -m src.ingestion.stream_manager

# Stream AAPL with custom config
python -m src.ingestion.stream_manager \
    --underlying AAPL \
    --expirations 5 \
    --strike-distance 20

# Test with limited iterations
python -m src.ingestion.stream_manager --max-iterations 10
```

**Note:** In production, use `main_engine` which handles streaming automatically.

---

## Database Schema

### Core Tables

#### `underlying_quotes`
Stores 1-minute aggregated underlying symbol quotes.

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | VARCHAR(10) | Underlying symbol (e.g., SPY) |
| `timestamp` | TIMESTAMPTZ | 1-minute bucket boundary (ET) |
| `open` | NUMERIC(12,4) | First price in bucket |
| `high` | NUMERIC(12,4) | Max price in bucket |
| `low` | NUMERIC(12,4) | Min price in bucket |
| `close` | NUMERIC(12,4) | Last price in bucket |
| `up_volume` | BIGINT | RAW cumulative uptick volume |
| `down_volume` | BIGINT | RAW cumulative downtick volume |

**Primary Key:** `(symbol, timestamp)`

#### `option_chains`
Stores 1-minute aggregated option contract data.

| Column | Type | Description |
|--------|------|-------------|
| `option_symbol` | VARCHAR(50) | TradeStation symbol (e.g., SPY 260221C450) |
| `timestamp` | TIMESTAMPTZ | 1-minute bucket boundary (ET) |
| `underlying` | VARCHAR(10) | Underlying symbol |
| `strike` | NUMERIC(12,4) | Strike price |
| `expiration` | DATE | Expiration date |
| `option_type` | CHAR(1) | 'C' (call) or 'P' (put) |
| `last` | NUMERIC(12,4) | Last trade price |
| `bid` | NUMERIC(12,4) | Bid price |
| `ask` | NUMERIC(12,4) | Ask price |
| `volume` | BIGINT | RAW cumulative volume |
| `open_interest` | BIGINT | RAW open interest |
| `implied_volatility` | NUMERIC(8,6) | IV (if available) |
| `delta` | NUMERIC(8,6) | Option delta (if available) |
| `gamma` | NUMERIC(10,8) | Option gamma (if available) |
| `theta` | NUMERIC(10,6) | Option theta (if available) |
| `vega` | NUMERIC(10,6) | Option vega (if available) |

**Primary Key:** `(option_symbol, timestamp)`

### Helper Views

#### `underlying_quotes_with_deltas`
Automatically calculates volume deltas using `LAG()` window function.

```sql
SELECT * FROM underlying_quotes_with_deltas 
WHERE symbol = 'SPY' 
ORDER BY timestamp DESC LIMIT 10;
```

#### `option_chains_with_deltas`
Automatically calculates volume and OI deltas using `LAG()` window function.

```sql
SELECT * FROM option_chains_with_deltas 
WHERE underlying = 'SPY' 
  AND expiration = '2026-02-21'
ORDER BY timestamp DESC LIMIT 10;
```

### Future Tables

- **`gex_calculations`** - Calculated gamma exposure per strike
- **`data_quality_log`** - Data quality issues and gaps
- **`ingestion_metrics`** - Pipeline performance metrics

---

## Development

### Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests (when implemented)
pytest

# Run with coverage
pytest --cov=src
```

### Code Formatting

```bash
# Format code with black
black src/

# Check linting
flake8 src/
```

### Project Structure

```
zerogex-oa/
├── src/
│   ├── __init__.py
│   ├── config.py                    # Centralized configuration
│   ├── validation.py                # Data validation utilities
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── tradestation_auth.py     # OAuth2 authentication
│   │   ├── tradestation_client.py   # Market data API client (with retry)
│   │   ├── backfill_manager.py      # Historical data fetching
│   │   ├── stream_manager.py        # Real-time data fetching
│   │   └── main_engine.py           # Orchestration + storage + aggregation
│   ├── database/
│   │   ├── __init__.py
│   │   ├── connection.py            # Connection pool management
│   │   └── password_providers.py   # Password retrieval plugins
│   └── utils/
│       ├── __init__.py
│       └── logging.py               # Centralized logging config
├── sql/
│   └── 001_create_tables.sql        # Database schema
├── scripts/
│   └── get_tradestation_tokens.py   # OAuth token generator
├── pyproject.toml                    # Project metadata & dependencies
├── .env.example                      # Environment template
├── .env                              # Your local config (git-ignored)
└── README.md                         # This file
```

---

## Environment Variables

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `TRADESTATION_CLIENT_ID` | TradeStation API client ID | `abc123...` |
| `TRADESTATION_CLIENT_SECRET` | TradeStation API client secret | `xyz789...` |
| `TRADESTATION_REFRESH_TOKEN` | OAuth2 refresh token | `def456...` |

### Database Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | Database host | `localhost` |
| `DB_PORT` | Database port | `5432` |
| `DB_NAME` | Database name | `zerogex` |
| `DB_USER` | Database user | `postgres` |
| `DB_PASSWORD_PROVIDER` | Password provider (`env` or `aws_secrets_manager`) | `aws_secrets_manager` |
| `DB_PASSWORD` | Database password (if using `env` provider) | - |
| `DB_SECRET_NAME` | AWS secret name (if using `aws_secrets_manager`) | `zerogex/db/password` |
| `AWS_REGION` | AWS region | `us-east-1` |

### API Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `API_REQUEST_TIMEOUT` | Request timeout (seconds) | `30` |
| `API_RETRY_ATTEMPTS` | Number of retries | `3` |
| `API_RETRY_DELAY` | Initial retry delay (seconds) | `1.0` |
| `API_RETRY_BACKOFF` | Exponential backoff multiplier | `2.0` |
| `QUOTE_BATCH_SIZE` | Quote batch size | `100` |
| `OPTION_BATCH_SIZE` | Option batch size | `100` |

### Streaming Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `MARKET_HOURS_POLL_INTERVAL` | Poll interval during market hours (seconds) | `5` |
| `EXTENDED_HOURS_POLL_INTERVAL` | Poll interval during extended hours (seconds) | `30` |
| `CLOSED_HOURS_POLL_INTERVAL` | Poll interval when market closed (seconds) | `300` |
| `STRIKE_RECALC_INTERVAL` | Strike recalculation frequency (iterations) | `10` |
| `PRICE_MOVE_THRESHOLD` | Price change to trigger recalc (dollars) | `1.0` |

### Aggregation Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `AGGREGATION_BUCKET_SECONDS` | Time bucket size (seconds) | `60` |
| `MAX_BUFFER_SIZE` | Max buffer before flush | `1000` |
| `BUFFER_FLUSH_INTERVAL` | Flush timeout (seconds) | `60` |

### Feature Flags

| Variable | Description | Default |
|----------|-------------|---------|
| `BACKFILL_ON_STARTUP` | Auto-backfill gaps on startup | `true` |
| `WEBSOCKET_ENABLED` | Use WebSocket streaming | `false` |
| `GEX_ENABLED` | Enable GEX calculations | `false` |
| `METRICS_ENABLED` | Enable Prometheus metrics | `false` |

See `.env.example` for complete list of configuration options.

---

## Monitoring & Observability

### Logging

All components use centralized logging with configurable levels:

```bash
# Set log level
LOG_LEVEL=DEBUG  # DEBUG, INFO, WARNING, ERROR, CRITICAL
```

**Log files (production):**
- `/var/log/zerogex/main_engine.log`
- `/var/log/zerogex/errors.log`

### Metrics (Future)

When `METRICS_ENABLED=true`:

```bash
# Prometheus endpoint
curl http://localhost:9090/metrics
```

**Available metrics:**
- `bars_stored_total` - Total underlying bars stored
- `options_stored_total` - Total option quotes stored
- `api_requests_total` - API requests by endpoint
- `api_latency_seconds` - API request latency histogram
- `buffer_size` - Current buffer sizes
- `errors_total` - Errors by type

### Grafana Dashboards (Future)

When `GRAFANA_DASHBOARD_ENABLED=true`:

**Dashboards:**
1. **Ingestion Pipeline** - Throughput, latency, error rates
2. **Data Quality** - Gaps, staleness, validation failures
3. **GEX Analysis** - Real-time gamma exposure visualization
4. **System Health** - CPU, memory, database connections

---

## Troubleshooting

### Authentication Errors

**Error:** `ValueError: Client ID, Client Secret, and Refresh Token are required`

**Solution:** Ensure `.env` has all credentials:
```bash
TRADESTATION_CLIENT_ID=your_actual_client_id
TRADESTATION_CLIENT_SECRET=your_actual_secret
TRADESTATION_REFRESH_TOKEN=your_actual_token
```

### Database Connection Errors

**Error:** `Failed to connect to database`

**Solutions:**
1. Check PostgreSQL is running: `sudo systemctl status postgresql`
2. Verify credentials in `.env`
3. Check database exists: `psql -l | grep zerogex`
4. For AWS Secrets Manager, verify IAM permissions

### Import Errors

**Error:** `ModuleNotFoundError: No module named 'src'`

**Solution:** Install in editable mode:
```bash
pip install -e .
```

### API Rate Limiting

**Error:** `429 Too Many Requests`

**Solutions:**
- Increase poll intervals in `.env`:
  ```bash
  MARKET_HOURS_POLL_INTERVAL=10
  EXTENDED_HOURS_POLL_INTERVAL=60
  ```
- Increase batch delays:
  ```bash
  DELAY_BETWEEN_BATCHES=1.0
  DELAY_BETWEEN_BARS=2.0
  ```
- Retry logic handles this automatically with exponential backoff

### Memory Issues

**Error:** High memory usage over time

**Solutions:**
- Decrease `MAX_BUFFER_SIZE`:
  ```bash
  MAX_BUFFER_SIZE=500
  ```
- Verify strike cleanup is running:
  ```bash
  STRIKE_CLEANUP_INTERVAL=50  # Clean more frequently
  ```
- Check for database connection leaks in logs

### Data Quality Issues

**Missing data:**
1. Check logs for API errors
2. Run gap detection:
   ```sql
   SELECT * FROM data_quality_log 
   WHERE resolved = false 
   ORDER BY check_timestamp DESC;
   ```
3. Manual backfill:
   ```bash
   python -m src.ingestion.backfill_manager --lookback-days 1
   ```

**Stale data:**
1. Check if streaming is running:
   ```bash
   sudo systemctl status zerogex-ingest
   ```
2. Check market hours - no updates when market closed
3. Review logs for streaming errors

### Debug Logging

Enable verbose logging for troubleshooting:

```bash
# Set in .env
LOG_LEVEL=DEBUG

# Or via command line
python -m src.ingestion.main_engine --debug
```

---

## Roadmap

### ✅ Completed (v0.1.0)
- [x] TradeStation API integration
- [x] Real-time data streaming
- [x] Historical data backfilling
- [x] 1-minute data aggregation
- [x] Database storage (PostgreSQL/TimescaleDB)
- [x] Data validation and error handling
- [x] Retry logic with exponential backoff
- [x] Dynamic polling based on market hours
- [x] Memory leak prevention
- [x] Graceful shutdown handling

### 🚧 In Progress (v0.2.0)
- [ ] WebSocket streaming implementation
- [ ] Automated gap backfilling
- [ ] Data quality monitoring
- [ ] Prometheus metrics integration

### 📋 Planned (v0.3.0)
- [ ] Real-time GEX calculation engine
- [ ] Greeks calculation (Black-Scholes)
- [ ] Grafana dashboards
- [ ] Alert system for GEX thresholds
- [ ] Customer-facing web UI

### 🔮 Future (v1.0.0)
- [ ] Multi-underlying support (QQQ, IWM, etc.)
- [ ] Advanced GEX analytics (zero-gamma levels, flip zones)
- [ ] Historical GEX backtesting
- [ ] Machine learning for GEX predictions
- [ ] Mobile app for alerts
- [ ] API for customers

---

## API Documentation

- **TradeStation API Docs:** https://api.tradestation.com/docs/
- **OAuth2 Authentication:** https://api.tradestation.com/docs/fundamentals/authentication/
- **Market Data Specification:** https://api.tradestation.com/docs/specification/#tag/MarketData

---

## Contributing

This is a private project. For questions or contributions, contact the maintainer.

---

## License

MIT License - See [LICENSE](LICENSE) file for details.

---

## Contact

**Author:** ZeroGEX, LLC  
**Email:** zerogexoptions@gmail.com  
**Project:** ZeroGEX Options Analytics Platform  
**Status:** Active Development

---

## Acknowledgments

- TradeStation for providing comprehensive market data API
- TimescaleDB team for time-series database optimization
- Python community for excellent libraries (requests, psycopg2, pytz)

---

**Built with:** Python • TradeStation API • PostgreSQL • TimescaleDB • AWS

**Deploy with confidence** - ZeroGEX is production-ready! 🚀
