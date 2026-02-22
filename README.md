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
  - [TradeStation Client](#tradestation-client)
  - [Streaming Engine](#streaming-engine)
  - [Backfill Engine](#backfill-engine)
- [Development](#development)
- [Project Structure](#project-structure)
- [Environment Variables](#environment-variables)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Features

### Core Capabilities
- ✅ **Real-time Options Data Streaming** - Live underlying quotes and options chain data
- ✅ **Historical Data Backfilling** - Configurable lookback periods for analysis
- ✅ **Intelligent Strike Selection** - Automatically tracks strikes near current price
- ✅ **Multi-Expiration Support** - Configurable number of expiration dates
- ✅ **Market Hours Detection** - Smart warnings for off-market data requests
- ✅ **Comprehensive API Coverage** - Full TradeStation Market Data API v3 support

### TradeStation API Integration
- Quote snapshots (equity & options)
- Historical OHLCV bars (1min, 5min, daily, etc.)
- Options chain data (expirations, strikes, quotes)
- Symbol search and metadata
- Market depth (Level 2)
- Cryptocurrency support

---

## Architecture

```
ZeroGEX Platform
├── Data Ingestion Layer (TradeStation API)
│   ├── Authentication (OAuth2 refresh token)
│   ├── Market Data Client (REST API)
│   ├── Streaming Engine (Real-time)
│   └── Backfill Engine (Historical)
├── Data Storage Layer
│   ├── PostgreSQL (relational data)
│   └── TimescaleDB (time-series optimization)
└── Analytics Layer (Future)
    ├── GEX Calculation Engine
    ├── Monitoring Dashboards
    └── Customer-Facing UI
```

---

## Prerequisites

### Required
- **Python 3.8+** (tested on 3.10)
- **TradeStation API Account** - [Sign up here](https://api.tradestation.com/docs/)
  - Client ID
  - Client Secret
  - Refresh Token

### Optional
- **PostgreSQL 12+** (for data persistence)
- **TimescaleDB** (for time-series optimization)
- **AWS EC2** (for production deployment)

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

The project uses `pyproject.toml` for dependency management. Install all dependencies with:

```bash
# Install the package in editable mode with all dependencies
pip install -e .

# Or if you want to install with development dependencies
pip install -e ".[dev]"
```

This will install:
- `requests` - HTTP client for API calls
- `python-dotenv` - Environment variable management
- `psycopg2-binary` - PostgreSQL adapter
- `pandas` - Data manipulation
- `websocket-client` - WebSocket support

**Development dependencies** (optional):
- `pytest` - Testing framework
- `black` - Code formatter
- `flake8` - Linter

### 4. Verify Installation

```bash
# Check that the package is installed
pip list | grep zerogex

# Should show:
# zerogex    0.1.0    /path/to/zerogex-oa
```

---

## Configuration

### 1. Create Environment File

```bash
# Copy the example environment file
cp .env.example .env
```

### 2. Configure TradeStation API Credentials

Edit `.env` and add your TradeStation API credentials:

```bash
# Required: TradeStation API Credentials
TRADESTATION_CLIENT_ID=your_client_id_here
TRADESTATION_CLIENT_SECRET=your_client_secret_here
TRADESTATION_REFRESH_TOKEN=your_refresh_token_here

# Optional: Use sandbox for testing
TRADESTATION_USE_SANDBOX=false

# Optional: Logging level
LOG_LEVEL=INFO
```

### 3. Get TradeStation API Credentials

1. Go to [TradeStation Developer Portal](https://api.tradestation.com/docs/)
2. Create a new application
3. Copy your **Client ID** and **Client Secret**
4. Generate a **Refresh Token** following [TradeStation's OAuth guide](https://api.tradestation.com/docs/fundamentals/authentication/refresh-tokens)

---

## Usage

### TradeStation Client

Test the TradeStation API connection:

```bash
# Run all tests
python -m src.ingestion.tradestation_client

# Test specific endpoint
python -m src.ingestion.tradestation_client --test quote --symbol SPY

# Enable debug logging
python -m src.ingestion.tradestation_client --debug

# Get help
python -m src.ingestion.tradestation_client --help
```

**Available tests:**
- `quote` - Get current quotes
- `bars` - Fetch historical OHLCV bars
- `options` - Options expirations and strikes
- `search` - Symbol search
- `market-hours` - Market status and hours
- `depth` - Level 2 market depth

### Streaming Engine

Stream real-time underlying quotes and options chain data:

```bash
# Stream SPY with defaults (3 expirations, ±$10 strikes, 5s interval)
python -m src.ingestion.streaming_engine

# Custom configuration
python -m src.ingestion.streaming_engine \
    --underlying AAPL \
    --expirations 5 \
    --strike-distance 20 \
    --interval 2

# Debug mode to see all data
python -m src.ingestion.streaming_engine --debug

# Test with limited iterations
python -m src.ingestion.streaming_engine --max-iterations 10 --debug

# Get help
python -m src.ingestion.streaming_engine --help
```

**Configuration Options:**
- `--underlying` - Symbol to track (default: SPY)
- `--expirations` - Number of expiration dates (default: 3)
- `--strike-distance` - Distance from ATM in dollars (default: 10.0)
- `--interval` - Poll interval in seconds (default: 5)
- `--max-iterations` - Limit iterations for testing (default: infinite)
- `--debug` - Enable debug logging

### Backfill Engine

Backfill historical underlying and options data:

```bash
# Backfill last 1 day of 5-minute data
python -m src.ingestion.backfill_engine

# Backfill last 3 days
python -m src.ingestion.backfill_engine --lookback-days 3

# Daily bars for 30 days
python -m src.ingestion.backfill_engine --unit Daily --lookback-days 30

# Sample options every 10 bars (faster, fewer API calls)
python -m src.ingestion.backfill_engine --sample-every 10

# Debug mode
python -m src.ingestion.backfill_engine --debug

# Get help
python -m src.ingestion.backfill_engine --help
```

**Configuration Options:**
- `--underlying` - Symbol to backfill (default: SPY)
- `--lookback-days` - Days to look back (default: 1)
- `--interval` - Bar interval (default: 5)
- `--unit` - Time unit: Minute, Daily, Weekly, Monthly (default: Minute)
- `--expirations` - Number of expirations (default: 3)
- `--strike-distance` - Strike distance from price (default: 10.0)
- `--sample-every` - Sample options every N bars (default: 1)
- `--debug` - Enable debug logging

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
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── tradestation_auth.py      # OAuth2 authentication
│   │   ├── tradestation_client.py    # Market data API client
│   │   ├── streaming_engine.py       # Real-time data streaming
│   │   └── backfill_engine.py        # Historical data backfill
│   └── utils/
│       ├── __init__.py
│       └── logging.py                # Centralized logging config
├── pyproject.toml                    # Project metadata & dependencies
├── setup.py                          # Setup configuration
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

### Optional Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `TRADESTATION_USE_SANDBOX` | Use sandbox/simulation environment | `false` |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL) | `INFO` |
| `TS_WARN_MARKET_HOURS` | Show market closed warnings | `true` |

### Test Configuration (TradeStation Client)

| Variable | Description | Default |
|----------|-------------|---------|
| `TS_TEST` | Which test to run | `all` |
| `TS_SYMBOL` | Symbol(s) to test | `SPY` |
| `TS_BARS_BACK` | Number of bars to retrieve | `5` |
| `TS_INTERVAL` | Bar interval | `1` |
| `TS_UNIT` | Time unit (Minute, Daily, Weekly, Monthly) | `Daily` |
| `TS_QUERY` | Search query for symbol search | `Apple` |

### Streaming Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `STREAM_UNDERLYING` | Underlying symbol | `SPY` |
| `STREAM_EXPIRATIONS` | Number of expirations | `3` |
| `STREAM_STRIKE_DISTANCE` | Strike distance from price | `10.0` |
| `STREAM_POLL_INTERVAL` | Seconds between polls | `5` |

### Backfill Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `BACKFILL_UNDERLYING` | Underlying symbol | `SPY` |
| `BACKFILL_LOOKBACK_DAYS` | Days to look back | `1` |
| `BACKFILL_INTERVAL` | Bar interval | `5` |
| `BACKFILL_UNIT` | Time unit | `Minute` |
| `BACKFILL_EXPIRATIONS` | Number of expirations | `3` |
| `BACKFILL_STRIKE_DISTANCE` | Strike distance | `10.0` |
| `BACKFILL_SAMPLE_EVERY` | Sample every N bars | `1` |

See `.env.example` for a complete configuration template.

---

## Troubleshooting

### Authentication Errors

**Error:** `ValueError: Client ID, Client Secret, and Refresh Token are required`

**Solution:** Make sure your `.env` file has all required credentials:
```bash
TRADESTATION_CLIENT_ID=your_actual_client_id
TRADESTATION_CLIENT_SECRET=your_actual_secret
TRADESTATION_REFRESH_TOKEN=your_actual_token
```

### Import Errors

**Error:** `ModuleNotFoundError: No module named 'src'`

**Solution:** Install the package in editable mode:
```bash
pip install -e .
```

### API Rate Limiting

**Error:** `429 Too Many Requests`

**Solution:** 
- Increase poll intervals: `--interval 10`
- Reduce option sampling: `--sample-every 5`
- Use sandbox for testing: `TRADESTATION_USE_SANDBOX=true`

### Market Hours Warnings

**Warning:** `⚠️  Market is currently closed - quotes may be delayed or stale`

**Solution:** This is informational. To suppress:
```bash
# In .env
TS_WARN_MARKET_HOURS=false
```

### Debug Logging

For any issues, enable debug logging to see detailed API calls:

```bash
# Set in .env
LOG_LEVEL=DEBUG

# Or via command line
python -m src.ingestion.streaming_engine --debug
```

---

## API Documentation

- **TradeStation API Docs:** https://api.tradestation.com/docs/
- **OAuth2 Authentication:** https://api.tradestation.com/docs/fundamentals/authentication/
- **Market Data Specification:** https://api.tradestation.com/docs/specification/#tag/MarketData

---

## Roadmap

- [ ] WebSocket streaming implementation
- [ ] Real-time GEX calculation engine
- [ ] PostgreSQL/TimescaleDB integration
- [ ] Monitoring dashboards (Chart.js/Grafana)
- [ ] Customer-facing web UI
- [ ] Automated systemd service deployment
- [ ] Alert system for GEX thresholds
- [ ] Historical GEX backtesting

---

## Contributing

This is a private project. For questions or contributions, contact the maintainer.

---

## License

Proprietary - All Rights Reserved

---

## Contact

**Author:** Michael  
**Project:** ZeroGEX Options Analytics Platform  
**Status:** Active Development

---

**Built with:** Python • TradeStation API • TimescaleDB • AWS EC2
