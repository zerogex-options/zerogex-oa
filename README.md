# ZeroGEX - Options Analytics Platform

**Real-time gamma exposure (GEX) calculations for options using TradeStation API integration.**

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
  - [Greeks & IV Calculator](#greeks--iv-calculator)
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
- **Implied Volatility Calculation** - Newton-Raphson solver calculates IV from option prices when API doesn't provide it
- **Real-time Greeks Calculation** - Black-Scholes Greeks (Delta, Gamma, Theta, Vega) calculated for every option
- **Intelligent Strike Selection** - Automatically tracks strikes near current price with dynamic recalculation
- **Multi-Expiration Support** - Configurable number of expiration dates to track
- **Market Hours Detection** - Dynamic polling intervals based on market session (regular/extended/closed)
- **Robust Error Handling** - Exponential backoff retry logic for API failures
- **Data Validation** - Comprehensive validation of all API responses
- **Database Storage** - PostgreSQL/TimescaleDB with proper timezone handling
- **Memory Management** - Automatic cleanup of expired strikes to prevent memory leaks
- **Graceful Shutdown** - Proper buffer flushing and connection cleanup on shutdown
- **Real-time Analytics Engine** - Calculates real-time gamma exposure, max pain, and second order Greeks

### 🚀 Future Enhancements (Foundation Ready)
- **WebSocket Streaming** - Low-latency real-time data (config ready)
- **Data Quality Monitoring** - Automated detection of stale/missing data (schema ready)
- **Grafana Dashboards** - Real-time visualization and alerting (metrics schema ready)

### 📊 TradeStation API Integration
- Quote snapshots (equity & options)
- Historical OHLCV bars (1min, 5min, daily, etc.)
- Stream bars with Up/Down volume breakdown
- Options chain data (expirations, strikes, quotes)
- Symbol search and metadata
- Market depth (Level 2)
- Retry logic with exponential backoff
- Configurable timeouts and batch sizes

### 📐 Greeks & Implied Volatility
- **IV Calculation**: Newton-Raphson method solves for IV from option prices
- **Greeks**: Delta, Gamma, Theta, Vega using Black-Scholes model
- **Priority Logic**: 
  1. Use IV from API if available
  2. Calculate IV from bid/ask mid-price
  3. Calculate IV from last price
  4. Fall back to configurable default IV
- **Robust Validation**: Checks for intrinsic value violations, constrains IV to reasonable ranges
- **Real-time Storage**: Both IV and Greeks stored in database for historical analysis

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
├── Analytics Layer
│   ├── IV Calculator (Newton-Raphson solver)
│   ├── Greeks Calculator (Black-Scholes model)
|   └── GEX, max pain and second order Greeks Calculation Engine
├── Data Storage Layer
│   ├── PostgreSQL (relational data)
│   ├── TimescaleDB (time-series optimization)
│   └── Helper views for delta calculations
├── Validation & Configuration
│   ├── Data validation utilities
│   ├── Timezone handling (pytz)
│   └── Centralized configuration
└── Future Layers
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
IVCalculator (calculate IV from prices if not provided)
      ↓
GreeksCalculator (calculate Greeks using IV)
      ↓
IngestionEngine (aggregate in 1-minute buckets)
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

# Or install everything including Greeks/IV calculation
pip install -e ".[all]"
```

**Core dependencies:**
- `requests` - HTTP client for API calls
- `python-dotenv` - Environment variable management
- `psycopg2-binary` - PostgreSQL adapter
- `pandas` - Data manipulation
- `websocket-client` - WebSocket support (future)
- `boto3` - AWS SDK (for Secrets Manager)
- `pytz` - Timezone handling
- `numpy` - Numerical computing
- `scipy` - Scientific computing (for Greeks & IV)

### 4. Set Up Database

```bash
# Create database
createdb zerogex

# Run schema migration
psql -d zerogex -f setup/database/schema.sql
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

### 4. Configure Greeks & IV Calculation

```bash
# Enable/disable features
GREEKS_ENABLED=true
IV_CALCULATION_ENABLED=true

# IV solver parameters
IV_MAX_ITERATIONS=100      # Max Newton-Raphson iterations
IV_TOLERANCE=0.00001       # Convergence tolerance
IV_MIN=0.01                # Minimum IV (1%)
IV_MAX=5.0                 # Maximum IV (500%)

# Greeks parameters
RISK_FREE_RATE=0.05                # 5% annual risk-free rate
IMPLIED_VOLATILITY_DEFAULT=0.20    # 20% default IV
```

### 5. Get TradeStation API Credentials

1. Go to [TradeStation Developer Portal](https://api.tradestation.com/docs/)
2. Create a new application
3. Copy your **Client ID** and **Client Secret**
4. Generate a **Refresh Token**:

```bash
python setup/app/get_tradestation_tokens.py
```

### 6. Review Configuration Options

See [Environment Variables](#environment-variables) section for complete configuration options.

---

## Usage

### Main Ingestion Engine

**The primary entry point for the complete data pipeline:**

```bash
# Run with defaults (streams real-time data)
python -m src.ingestion.main_engine

# Custom configuration
python -m src.ingestion.main_engine \
    --underlying SPY \
    --expirations 5 \
    --strike-distance 20.0

# Debug mode
python -m src.ingestion.main_engine --debug

# Get help
python -m src.ingestion.main_engine --help
```

**What it does:**
1. **Streaming Phase** - Streams real-time data with dynamic polling
2. **IV Calculation** - Calculates implied volatility from option prices (if enabled)
3. **Greeks Calculation** - Calculates Delta, Gamma, Theta, Vega (if enabled)
4. **1-Minute Aggregation** - Buffers and aggregates data into 1-minute bars
5. **Database Storage** - Stores aggregated data with IV and Greeks in PostgreSQL

**Production deployment:**

The systemd service file is already provided in `setup/systemd/zerogex-oa-ingestion.service`. Use the Makefile for easy service management:

```bash
# Service Management
make ingestion-start    # Start the service
make ingestion-stop     # Stop the service
make ingestion-restart  # Restart the service
make ingestion-status   # Check service status
make ingestion-enable   # Enable on boot
make ingestion-disable  # Disable on boot
make ingestion-health   # Health check with errors/warnings

# Log Management
make ingestion-logs         # Watch live logs (Ctrl+C to stop)
make ingestion-logs-tail    # Show last 100 lines
make ingestion-logs-errors  # Show recent errors only
make logs-clear             # Clear all logs (with confirmation)
```

**Initial setup:**

```bash
# Install the systemd service
sudo cp setup/systemd/zerogex-oa-ingestion.service /etc/systemd/system/
sudo systemctl daemon-reload

# Enable and start
make ingestion-enable
make ingestion-start

# Watch logs
make ingestion-logs
```

---

### TradeStation Client

**Test TradeStation API connection and endpoints:**

```bash
# Run all tests
make run-client

# Test with specific options (pass through to Python module)
make run-client

# For advanced testing, use Python directly:
python -m src.ingestion.tradestation_client --test quote --symbol SPY
python -m src.ingestion.tradestation_client --test bars --symbol SPY --bars-back 10
python -m src.ingestion.tradestation_client --test stream-bars --symbol SPY
python -m src.ingestion.tradestation_client --test options --symbol SPY
python -m src.ingestion.tradestation_client --debug
```

**Available tests:**
- `quote` - Current quotes
- `bars` - Historical OHLCV bars
- `stream-bars` - Real-time bars with Up/Down volume
- `options` - Option expirations and strikes
- `search` - Symbol search
- `market-hours` - Market status and hours
- `depth` - Level 2 market depth

---

### Backfill Manager

**Standalone historical data backfill:**

```bash
# Run backfill with defaults
make run-backfill

# For custom options, use Python directly:
python -m src.ingestion.backfill_manager --lookback-days 3
python -m src.ingestion.backfill_manager --unit Daily --lookback-days 30
python -m src.ingestion.backfill_manager --sample-every 10
```

**Note:** Backfill runs independently and stores data directly in the database.

---

### Stream Manager

**Standalone real-time streaming (for testing):**

```bash
# Test streaming
make run-stream

# For custom options, use Python directly:
python -m src.ingestion.stream_manager --underlying AAPL --expirations 5
python -m src.ingestion.stream_manager --max-iterations 10
```

**Note:** For production, use `make start` which runs the main engine automatically.

---

### Greeks & IV Calculator

**Test Greeks and IV calculation standalone:**

```bash
# Test Greeks calculator
make run-greeks

# Test IV calculator
make run-iv

# Test authentication
make run-auth

# Show current configuration
make run-config
```

**How it works in production:**

When the main engine is running with `GREEKS_ENABLED=true` and `IV_CALCULATION_ENABLED=true`:

1. **Option quote received** from TradeStation API
2. **IV Calculation** (if API doesn't provide IV):
   - Try to calculate from bid/ask mid-price
   - Fall back to last price
   - Use default IV if all else fails
3. **Greeks Calculation** using the IV:
   - Delta (rate of change with underlying price)
   - Gamma (rate of change of Delta)
   - Theta (time decay per day)
   - Vega (sensitivity to volatility)
4. **Storage** - Both IV and Greeks stored in database

**Example output:**
```
✅ First Greek calculated successfully: delta=0.5234, gamma=0.0123
✅ Calculated IV for SPY 260322C455: 0.2145 (21.45%)
✅ Stored option with Greeks: SPY 260322C455 delta=0.5234 gamma=0.0123
```

---

### Analytics Engine

**Independent GEX, Max Pain, and second-order Greeks calculator:**

```bash
# Run analytics engine (development)
make run-analytics

# Run once for testing
make run-analytics-once

# Query latest GEX summary
make gex-summary

# Query GEX by strike
make gex-strikes
```

**Production deployment:**

```bash
# Install systemd service
sudo cp setup/systemd/zerogex-analytics.service /etc/systemd/system/
sudo systemctl daemon-reload

# Enable and start
make analytics-enable
make analytics-start

# Watch logs
make analytics-logs

# Check status
make analytics-status
make analytics-health
```

**What it calculates:**

1. **Gamma Exposure (GEX)** by strike and in aggregate
   - Call GEX (positive for dealers who are short)
   - Put GEX (negative for dealers who are long)
   - Net GEX (call - put from dealer perspective)

2. **Gamma Flip Point** - Strike where net GEX crosses zero
   - Above: Dealers are long gamma (stabilizing market)
   - Below: Dealers are short gamma (destabilizing market)

3. **Max Pain** - Strike where option holders lose most value
   - Calculated by minimizing total option value across all strikes
   - Useful for understanding potential pinning behavior

4. **Second-Order Greeks** - Vanna and Charm
   - **Vanna**: How delta changes with volatility (∂²V/∂S∂σ)
   - **Charm**: How delta decays with time (∂²V/∂S∂T)

5. **Put/Call Ratios** - Volume and Open Interest based

**Key features:**

- **Decoupled from ingestion** - Runs independently on its own schedule
- **Configurable interval** - Default 60 seconds, adjustable via `ANALYTICS_INTERVAL`
- **Uses latest database data** - Works with whatever data is available
- **Dual timestamps** - Data timestamp + calculation timestamp via `created_at`
- **No market hours logic** - Just calculates against available data

**Example output:**
```
================================================================================
GEX SUMMARY
================================================================================
Max Gamma Strike: $455.00
Max Gamma Value: 2,547,893
Gamma Flip Point: $448.75
Max Pain: $450.00
Put/Call Ratio: 1.23
Total Net GEX: 1,234,567
================================================================================
```

**Configuration (.env):**
```bash
# Enable analytics
ANALYTICS_ENABLED=true

# Calculation interval (seconds)
ANALYTICS_INTERVAL=60

# Underlying to analyze
ANALYTICS_UNDERLYING=SPY

# Risk-free rate for Greeks
RISK_FREE_RATE=0.05
```

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
Stores 1-minute aggregated option contract data with IV and Greeks.

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
| `implied_volatility` | NUMERIC(8,6) | IV (calculated or from API) |
| `delta` | NUMERIC(8,6) | Option delta |
| `gamma` | NUMERIC(10,8) | Option gamma |
| `theta` | NUMERIC(10,6) | Option theta (per day) |
| `vega` | NUMERIC(10,6) | Option vega (per 1% IV) |

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
│   │   ├── tradestation_client.py   # Market data API client
│   │   ├── iv_calculator.py         # IV calculation (Newton-Raphson)
│   │   ├── greeks_calculator.py     # Black-Scholes Greeks
│   │   ├── backfill_manager.py      # Historical data fetching
│   │   ├── stream_manager.py        # Real-time data fetching
│   │   └── main_engine.py           # Orchestration + storage
│   ├── analytics/                    # ⭐ NEW
│   │   ├── __init__.py              # ⭐ NEW
│   │   └── main_engine.py           # ⭐ NEW - GEX & Max Pain calculator
│   ├── database/
│   │   ├── __init__.py
│   │   ├── connection.py            # Connection pool management
│   │   └── password_providers.py   # Password retrieval plugins
│   └── utils/
│       ├── __init__.py
│       └── logging.py               # Centralized logging config
├── setup/
│   ├── app/
│   │   └── get_tradestation_tokens.py
│   ├── database/
│   │   └── schema.sql               # Complete database schema
│   └── systemd/
│       ├── zerogex-oa-ingestion.service
│       └── zerogex-analytics.service # ⭐ NEW
├── pyproject.toml                    # Project metadata & dependencies
├── Makefile                          # Service management & DB queries
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
| `DB_NAME` | Database name | `zerogexdb` |
| `DB_USER` | Database user | `postgres` |
| `DB_PASSWORD_PROVIDER` | Password provider (`env`, `aws_secrets_manager`, `pgpass`) | `pgpass` |
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

### Analytics Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `ANALYTICS_UNDERLYING` | Underlying symbol to analyze | `SPY` |
| `ANALYTICS_INTERVAL` | Calculation interval in seconds | `60` |

**Note:** Analytics engine also uses `RISK_FREE_RATE` from the Greeks & IV Configuration section above.

### Greeks & IV Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `GREEKS_ENABLED` | Enable Greeks calculation | `true` |
| `IV_CALCULATION_ENABLED` | Enable IV calculation from prices | `true` |
| `IV_MAX_ITERATIONS` | Max Newton-Raphson iterations | `100` |
| `IV_TOLERANCE` | Convergence tolerance | `0.00001` |
| `IV_MIN` | Minimum IV | `0.01` (1%) |
| `IV_MAX` | Maximum IV | `5.0` (500%) |
| `RISK_FREE_RATE` | Risk-free rate for calculations | `0.05` (5%) |
| `IMPLIED_VOLATILITY_DEFAULT` | Default IV fallback | `0.20` (20%) |

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

### Makefile Shortcuts

ZeroGEX provides comprehensive Makefile commands for monitoring, service management, and database queries:

```bash
# Ingestion Service Management
make ingestion-start    # Start the ingestion service
make ingestion-stop     # Stop the ingestion service
make ingestion-restart  # Restart the ingestion service
make ingestion-status   # Show service status
make ingestion-enable   # Enable service to start on boot
make ingestion-disable  # Disable service from starting on boot
make ingestion-health   # Service health check with recent errors

# Analytics Service Management
make analytics-start    # Start the analytics service
make analytics-stop     # Stop the analytics service
make analytics-restart  # Restart the analytics service
make analytics-status   # Show service status
make analytics-enable   # Enable service to start on boot
make analytics-disable  # Disable service from starting on boot
make analytics-health   # Service health check with recent errors

# Log Management
make ingestion-logs         # Watch live ingestion logs (Ctrl+C to stop)
make ingestion-logs-tail    # Show last 100 ingestion log lines
make ingestion-logs-errors  # Show recent ingestion errors only
make analytics-logs         # Watch live analytics logs (Ctrl+C to stop)
make analytics-logs-tail    # Show last 100 analytics log lines
make analytics-logs-errors  # Show recent analytics errors only
make logs-grep PATTERN="Greeks"  # Search logs for pattern
make logs-clear             # Clear all journalctl logs (with confirmation)

# Quick Stats
make stats              # Overall data statistics
make latest             # Latest data from all tables
make today              # Today's data summary
make check-streaming    # Check if data is actively streaming

# Underlying Data
make underlying         # Last 10 underlying bars
make underlying-latest  # Latest underlying bar
make underlying-today   # Today's underlying bars
make underlying-volume  # Volume analysis for today

# Option Data
make options            # Last 10 option quotes
make options-latest     # Latest option quotes (top 10 by volume)
make options-today      # Today's option activity
make options-strikes    # Active strikes summary
make options-raw        # Raw option data
make options-fields     # Check field population (including IV/Greeks)

# Greeks & Analytics
make greeks             # Latest Greeks by strike
make greeks-summary     # Greeks summary statistics
make gex-summary        # Latest GEX summary ⭐ NEW
make gex-strikes        # GEX by strike (top 20) ⭐ NEW
make gex-preview

# Real-Time Flow Analysis (🔥 For Trading Decisions)
make flow-by-type       # Puts vs calls flow (all strikes/expirations)
make flow-by-strike     # Flow by strike level
make flow-by-expiration # Flow by expiration date
make flow-smart-money   # Unusual activity detection
make flow-buying-pressure # Underlying buying/selling pressure
make flow-live          # Combined real-time flow dashboard ⭐

# Data Quality
make gaps               # Check for data gaps
make gaps-today         # Today's data gaps
make quality            # Data quality report

# Data Management
make clear-data         # Clear all data (with confirmation)
make clear-options      # Clear only option chains
make clear-underlying   # Clear only underlying quotes

# Maintenance
make vacuum             # Vacuum analyze all tables
make size               # Show table sizes
make refresh-views      # Refresh materialized views

# Run Components
make run-auth           # Test TradeStation authentication
make run-client         # Test TradeStation API client
make run-backfill       # Run historical data backfill
make run-stream         # Test real-time streaming
make run-ingest         # Run main ingestion engine
make run-greeks         # Test Greeks calculator
make run-iv             # Test IV calculator
make run-config         # Show current configuration

# Interactive
make psql               # Open PostgreSQL shell
make query SQL="..."    # Run custom SQL query

# Get help
make help               # Show all available commands
```

### Real-Time Flow Analysis

ZeroGEX includes 5 real-time flow views for making trading decisions with **zero lag**:

#### 1. Flow by Type (`make flow-by-type`)
Shows aggregate puts vs calls flow across all strikes and expirations:
- Total call/put flow per minute
- Put/Call ratio
- Net flow (bullish/bearish indicator)
- Sentiment classification

**Use case:** Overall market sentiment, identifying put or call heavy periods

#### 2. Flow by Strike (`make flow-by-strike`)
Shows flow aggregated by strike level across all expirations:
- Which strikes are getting hit with flow
- Call/Put breakdown per strike
- Average Greeks at each strike level

**Use case:** Identifying key support/resistance levels, gamma walls, pin risk

#### 3. Flow by Expiration (`make flow-by-expiration`)
Shows flow aggregated by expiration date across all strikes:
- Which expiration cycles are active
- Days to expiry (DTE)
- Average IV and theta per expiration

**Use case:** Identifying where traders are positioning (0DTE, weeklies, monthlies)

#### 4. Smart Money Flow (`make flow-smart-money`)
Filters for unusual activity indicating informed trading:
- Large blocks (100+, 200+, 500+ contracts)
- High IV plays (>40%, >60%, >100% IV)
- Deep OTM unusual activity
- **Automatic unusual score (0-10)** for each trade

**Use case:** Spotting potential edge, following smart money, finding asymmetric setups

#### 5. Underlying Buying Pressure (`make flow-buying-pressure`)
Time series of buying vs selling pressure in the underlying:
- Buying pressure percentage
- Up/down volume breakdown
- Price momentum classification
- Period-over-period flow changes

**Use case:** Confirming directional bias, spotting divergences with options flow

#### Combined Dashboard (`make flow-live`)
Real-time dashboard showing all 5 views in one command:
```bash
make flow-live

# Shows:
# 1. Underlying buying pressure (last 10 bars)
# 2. Puts vs calls flow (last 10 minutes)
# 3. Smart money/unusual activity (top 10)
# 4. Top strikes by flow (top 10)
```

Perfect for keeping open in a terminal during trading hours!

### Common Workflows

**Start and monitor:**
```bash
make ingestion-start
make analytics-start
make ingestion-logs      # In one terminal
make analytics-logs      # In another terminal
```

**Real-time trading decisions:**
```bash
# Quick check on market sentiment
make flow-by-type

# See what strikes are getting hit
make flow-by-strike

# Look for unusual activity
make flow-smart-money

# Check GEX levels
make gex-summary
make gex-strikes

# Full dashboard
make flow-live

# Keep it updating every 10 seconds
watch -n 10 make flow-live
```

**Check data health:**
```bash
make ingestion-health
make analytics-health
make check-streaming
make quality
```

**Troubleshoot issues:**
```bash
make ingestion-logs-errors
make analytics-logs-errors
make greeks-summary
make options-fields
```

**Clear and restart:**
```bash
make ingestion-stop
make analytics-stop
make clear-data
make logs-clear
make ingestion-start
make analytics-start
```

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
- `greeks_calculated_total` - Total Greeks calculations
- `iv_calculated_total` - Total IV calculations
- `api_requests_total` - API requests by endpoint
- `api_latency_seconds` - API request latency histogram
- `buffer_size` - Current buffer sizes
- `errors_total` - Errors by type

---

## Troubleshooting

### Quick Diagnostics

```bash
# Check service status
make status
make health

# View recent logs
make logs-tail
make logs-errors

# Check data flow
make check-streaming
make stats
```

### Authentication Errors

**Error:** `ValueError: Client ID, Client Secret, and Refresh Token are required`

**Solution:** Ensure `.env` has all credentials:
```bash
TRADESTATION_CLIENT_ID=your_actual_client_id
TRADESTATION_CLIENT_SECRET=your_actual_secret
TRADESTATION_REFRESH_TOKEN=your_actual_token
```

**Test authentication:**
```bash
make run-auth
```

### Database Connection Errors

**Error:** `Failed to connect to database`

**Solutions:**
1. Check PostgreSQL is running: `sudo systemctl status postgresql`
2. Verify credentials in `.env`
3. Check database exists: `psql -l | grep zerogex`
4. For AWS Secrets Manager, verify IAM permissions

**Test connection:**
```bash
make psql
make query SQL="SELECT NOW();"
```

### Import Errors

**Error:** `ModuleNotFoundError: No module named 'src'`

**Solution:** Install in editable mode:
```bash
pip install -e .
```

### Greeks/IV Not Calculating

**Check configuration:**
```bash
make run-config
```

**Check in database:**
```bash
make options-fields
make greeks-summary
```

**Verify in logs:**
```bash
make logs-grep PATTERN="Greek"
make logs-grep PATTERN="IV"

# Should see:
# ✅ Greeks calculation ENABLED
# ✅ IV calculation ENABLED
# 🎯 First underlying price received: $450.23
# ✅ First Greek calculated successfully: delta=0.5234, gamma=0.0123
```

**Common issues:**
1. `GREEKS_ENABLED=false` in `.env`
2. `IV_CALCULATION_ENABLED=false` in `.env`
3. No underlying price available yet (wait for first bar)
4. Option prices invalid (zero or negative)

**Fix and restart:**
```bash
# Edit .env to enable features
nano .env

# Restart service
make restart
make logs
```

### Service Won't Start

**Check logs:**
```bash
make status
make logs-tail
make logs-errors
```

**Common issues:**
1. Invalid `.env` format (inline comments not allowed)
2. Missing dependencies: `pip install -e .`
3. Database not running: `sudo systemctl status postgresql`
4. Port already in use

**Clear logs and restart:**
```bash
make logs-clear
make restart
make logs
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
- Restart: `make restart`

### Memory Issues

**Error:** High memory usage over time

**Check memory:**
```bash
make health  # Shows memory usage
```

**Solutions:**
- Decrease `MAX_BUFFER_SIZE`:
  ```bash
  MAX_BUFFER_SIZE=500
  ```
- Verify strike cleanup is running:
  ```bash
  STRIKE_CLEANUP_INTERVAL=50
  ```
- Restart service: `make restart`

### Data Not Storing

**Check what's in database:**
```bash
make stats
make latest
make check-streaming
```

**Check service is running:**
```bash
make status
make logs-tail
```

**Look for errors:**
```bash
make logs-errors
make logs-grep PATTERN="Error|Exception"
```

**Nuclear option - clear and restart:**
```bash
make stop
make clear-data
make logs-clear
make start
make logs
```

### Debug Logging

Enable verbose logging for troubleshooting:

```bash
# Set in .env
LOG_LEVEL=DEBUG

# Restart service
make restart

# Watch debug logs
make logs
```

---

## Roadmap

### ✅ Completed (v0.2.0)
- [x] TradeStation API integration
- [x] Real-time data streaming with Up/Down volume
- [x] Historical data backfilling
- [x] 1-minute data aggregation
- [x] Database storage (PostgreSQL/TimescaleDB)
- [x] **Implied Volatility calculation (Newton-Raphson)**
- [x] **Black-Scholes Greeks calculation (Delta, Gamma, Theta, Vega)**
- [x] Data validation and error handling
- [x] Retry logic with exponential backoff
- [x] Dynamic polling based on market hours
- [x] Memory leak prevention
- [x] Graceful shutdown handling

### 🚧 In Progress (v0.3.0)
- [ ] WebSocket streaming implementation
- [ ] Automated gap backfilling
- [ ] Data quality monitoring
- [ ] Prometheus metrics integration

### 📋 Planned (v0.4.0)
- [ ] Real-time GEX calculation engine
- [ ] Vanna and Charm (2nd order Greeks)
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
- Python community for excellent libraries (requests, psycopg2, scipy, numpy)
- Black-Scholes model pioneers: Fischer Black, Myron Scholes, Robert Merton

---

**Built with:** Python • TradeStation API • PostgreSQL • TimescaleDB • Black-Scholes • Newton-Raphson • AWS

**Deploy with confidence** - ZeroGEX is production-ready! 🚀
