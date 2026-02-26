# ZeroGEX-OA Deployment Guide

Complete deployment automation for the ZeroGEX Options Analytics platform on a fresh Ubuntu server with AWS RDS PostgreSQL database.

## Overview

This deployment system automates the complete setup of ZeroGEX-OA including:
- System configuration and package installation
- AWS RDS PostgreSQL database connection and schema setup
- Python application and dependencies
- TradeStation API integration
- Systemd service configuration
- Security hardening
- Automated data retention (cleanup cron job)
- Deployment validation
- Installs FastAPI backend dependencies (fastapi, uvicorn, asyncpg, pydantic)
- Configures UFW firewall to allow port 8000
- Installs systemd service for API server
- Enables and starts API service
- Tests API health endpoint
- Provides REST API for frontend with 15+ endpoints
  - GEX metrics (summary, by-strike, historical)
  - Options flow (by type, by strike, smart money)
  - Day trading signals (VWAP, ORB, gamma levels, dealer hedging, volume spikes, momentum divergence)
  - Market data (current quote, historical)

## Prerequisites

- Ubuntu 20.04 or 22.04 LTS server (EC2 instance)
- Sudo access
- At least 4GB RAM (8GB+ recommended)
- 50GB+ storage (100GB+ recommended for production)
- **AWS RDS PostgreSQL instance** (already provisioned)
  - PostgreSQL 13+ recommended
  - Endpoint, port, database name, and master credentials
  - Security group allowing EC2 instance to connect
- TradeStation API credentials (Client ID, Secret, and Refresh Token)

## Quick Start

```bash
# 1. Clone the repository
cd ~
git clone <your-repo-url> zerogex-oa
cd zerogex-oa

# 2. Make deployment script executable
chmod +x deploy/deploy.sh
chmod +x deploy/steps/*

# 3. Run full deployment
./deploy/deploy.sh
```

## Deployment Steps

The deployment process runs these steps in order:

### Step 010: System Setup
- Updates system packages
- Installs essential tools (git, curl, python3, postgresql-client, etc.)
- Configures timezone to America/New_York
- Sets up .bashrc with ZeroGEX environment

### Step 015: Data Volume Setup
- Detects and mounts secondary storage volumes (if available)
- Creates `/data` mount point for monitoring and backups
- Falls back to root volume if no secondary volume available
- Creates `/data/monitoring` and `/data/backups` directories

### Step 020: Database Setup (AWS RDS)
- Installs PostgreSQL client tools for psql
- Prompts for AWS RDS connection details (endpoint, port, database, credentials)
- Tests database connection
- Creates `.pgpass` file for passwordless access
- Checks for TimescaleDB extension (optional)
- Applies database schema from `setup/database/schema.sql`
- Verifies core tables are created
- **Sets up data retention cron job** (runs daily at 2:00 AM ET)
  - Cleans up data older than retention policy (90 days)
  - Logs to `/var/log/zerogex/cleanup.log`

### Step 030: Application Setup
- Creates Python virtual environment
- Installs zerogex package in editable mode
- Installs all dependencies (core + optional)
- Creates `.env` file from template
- Prompts for TradeStation API credentials (Client ID and Secret)

### Step 040: TradeStation Auth
- Interactive script to obtain TradeStation OAuth tokens
- Guides through authorization code flow
- Saves refresh token to `.env` file

### Step 050: Security Hardening
- Configures UFW firewall (SSH only by default)
- Hardens SSH (disables root login, password auth)
- Sets up fail2ban for brute-force protection
- Enables automatic security updates (with auto-reboot at 3:00 AM)

### Step 100: Systemd Services
- Installs systemd service files:
  - `zerogex-oa-ingestion.service` - Data ingestion engine
  - `zerogex-oa-analytics.service` - GEX analytics engine
- Enables services to start on boot
- Starts services and verifies status

### Step 110: API Server Setup
- Installs API dependencies
- Configures firewall (port 8000)
- Sets up systemd service
- Provides REST API endpoints
- Interactive API docs at /docs

### Step 200: Validation
- Comprehensive deployment validation with RDS connection
- Checks service status
- Verifies database connectivity using `.pgpass` credentials
- Validates schema (8 core tables, 13 real-time views)
- Confirms data retention policies and cron job
- Tests Python environment and packages
- Checks configuration file permissions
- Reports summary with pass/fail/warnings

**Validation Checks Include:**
- Service status (ingestion, analytics)
- RDS database connection
- Core tables: symbols, underlying_quotes, option_chains, gex_summary, gex_by_strike, data_quality_log, ingestion_metrics, data_retention_policy
- Real-time views: deltas, flow analysis, VWAP, ORB, gamma levels, dealer hedging, volume spikes, momentum divergence
- Data retention configuration
- Cleanup cron job
- Python packages (psycopg2, pandas, numpy, scipy, requests, pytz)
- Configuration files (.pgpass, .env) with secure permissions (600)

## Partial Deployment

You can start deployment from any step:

```bash
# Start from database setup
./deploy/deploy.sh --start-from 020

# Start from application setup
./deploy/deploy.sh --start-from 030

# Start from systemd services
./deploy/deploy.sh --start-from 100

# Start from validation
./deploy/deploy.sh --start-from 200
```

## Post-Deployment Configuration

### 1. Verify Database Connection

The deployment creates `~/.pgpass` with your RDS credentials:

```
<rds-endpoint>:5432:zerogex:postgres:<your-password>
```

Permissions are automatically set to `600` for security.

Test connection:
```bash
# Using .pgpass (no password prompt)
psql -h <rds-endpoint> -U postgres -d zerogex -c "SELECT NOW();"

# Or use Makefile
make psql
```

### 2. Verify .env Configuration

Check `~/zerogex-oa/.env`:

```bash
# Database (RDS connection via .pgpass)
DB_PASSWORD_PROVIDER=pgpass

# TradeStation API
TRADESTATION_CLIENT_ID=<set-during-deployment>
TRADESTATION_CLIENT_SECRET=<set-during-deployment>
TRADESTATION_REFRESH_TOKEN=<set-by-step-040>
TRADESTATION_USE_SANDBOX=false
```

### 3. Add SPY Symbol

```bash
cd ~/zerogex-oa
source venv/bin/activate

# Connect to RDS and add SPY
psql -h <rds-endpoint> -U postgres -d zerogex -c \
  "INSERT INTO symbols (symbol, name, asset_type) 
   VALUES ('SPY', 'SPDR S&P 500 ETF', 'ETF') 
   ON CONFLICT DO NOTHING;"

# Or use Makefile
make psql
# Then run the INSERT command
```

### 4. Test Data Ingestion

```bash
# Watch ingestion logs
make ingestion-logs

# Check recent data
make latest
make stats
```

### 5. Test API Server
```bash
# Check API is running
sudo systemctl status zerogex-oa-api

# Test health endpoint
curl http://localhost:8000/api/health | jq

# Test GEX endpoint
curl http://localhost:8000/api/gex/summary | jq

# Browse interactive API documentation
# Open in browser: http://your-server-ip:8000/docs
```

**Available API Endpoints:**
- Health: `/api/health`
- GEX Summary: `/api/gex/summary`
- GEX by Strike: `/api/gex/by-strike`
- Options Flow: `/api/flow/by-type`, `/api/flow/by-strike`, `/api/flow/smart-money`
- Day Trading: `/api/trading/vwap-deviation`, `/api/trading/opening-range`, `/api/trading/gamma-levels`, etc
.
- Market Data: `/api/market/quote`, `/api/market/historical`

## Service Management

### Using Makefile (Recommended)

```bash
# Ingestion Service
make ingestion-start
make ingestion-stop
make ingestion-restart
make ingestion-status
make ingestion-logs

# Analytics Service
make analytics-start
make analytics-stop
make analytics-restart
make analytics-status
make analytics-logs

# API Service
make api-start
make api-stop
make api-restart
make api-status
make api-logs
make api-test

# Quick Stats
make stats
make latest
make gex-summary
```

### Using Systemctl Directly

```bash
# Service control
sudo systemctl start zerogex-oa-ingestion
sudo systemctl stop zerogex-oa-ingestion
sudo systemctl restart zerogex-oa-ingestion
sudo systemctl status zerogex-oa-ingestion

sudo systemctl start zerogex-oa-analytics
sudo systemctl stop zerogex-oa-analytics
sudo systemctl restart zerogex-oa-analytics
sudo systemctl status zerogex-oa-analytics

sudo systemctl start zerogex-oa-api
sudo systemctl stop zerogex-oa-api
sudo systemctl restart zerogex-oa-api
sudo systemctl status zerogex-oa-api

# View logs
sudo journalctl -u zerogex-oa-ingestion -f
sudo journalctl -u zerogex-oa-analytics -f
sudo journalctl -u zerogex-oa-api -f
```

## Database Queries

### Quick Checks

```bash
# Show data statistics
make stats

# Show latest data
make latest

# Show today's summary
make today

# Show GEX summary
make gex-summary

# Show option flow
make flow-live
```

### Direct SQL

```bash
# Connect to database (uses .pgpass)
make psql

# Run custom query
make query SQL="SELECT COUNT(*) FROM underlying_quotes;"
```

## Data Retention & Cleanup

### Automated Cleanup

The deployment automatically configures a cron job that runs daily at 2:00 AM ET:

```bash
# View cron job
crontab -l | grep cleanup_old_data

# View cleanup logs
tail -f /var/log/zerogex/cleanup.log

# Test cleanup manually
psql -h <rds-endpoint> -U postgres -d zerogex -c "SELECT * FROM cleanup_old_data();"
```

### Retention Policies

Default retention (configurable in `data_retention_policy` table):
- `underlying_quotes`: 90 days
- `option_chains`: 90 days
- `gex_summary`: 90 days
- `gex_by_strike`: 90 days
- `data_quality_log`: 365 days
- `ingestion_metrics`: 30 days

View current policies:
```bash
make psql
SELECT * FROM data_retention_policy ORDER BY table_name;
```

### Modify Retention Policies

```bash
make psql

-- Update retention for a specific table
UPDATE data_retention_policy 
SET retention_days = 60 
WHERE table_name = 'underlying_quotes';

-- Disable cleanup for a table
UPDATE data_retention_policy 
SET enabled = false 
WHERE table_name = 'option_chains';

-- Re-enable cleanup
UPDATE data_retention_policy 
SET enabled = true 
WHERE table_name = 'option_chains';
```

## Troubleshooting

### Services Won't Start

Check logs:
```bash
make ingestion-logs-errors
make analytics-logs-errors

# Or use systemctl
sudo journalctl -u zerogex-oa-ingestion -n 50
```

Common issues:
- TradeStation credentials not configured in `.env`
- Database connection issues (check RDS security group)
- Missing `.pgpass` file or wrong permissions
- AWS RDS endpoint unreachable from EC2 instance

### Database Connection Errors

Verify .pgpass file:
```bash
# Check permissions (must be 600)
ls -la ~/.pgpass

# Fix permissions if needed
chmod 600 ~/.pgpass

# View contents (should show RDS endpoint, not localhost)
cat ~/.pgpass
```

Test RDS connection:
```bash
# Should connect without password prompt
psql -h <rds-endpoint> -U postgres -d zerogex -c "SELECT NOW();"

# If fails, check:
# 1. RDS endpoint is correct
# 2. RDS security group allows EC2 instance
# 3. Network connectivity
ping <rds-endpoint>
```

Check RDS security group:
- Inbound rule allowing PostgreSQL (port 5432) from EC2 instance's security group
- Or allow from EC2 instance's private IP address

### No Data Appearing

1. Check market hours (data only flows when markets are open)
2. Verify TradeStation API connection:
```bash
cd ~/zerogex-oa
source venv/bin/activate
python -m src.ingestion.tradestation_client --test quote
```

3. Check ingestion service is running:
```bash
make ingestion-status
sudo journalctl -u zerogex-oa-ingestion -f
```

4. Verify SPY symbol exists:
```bash
make psql
SELECT * FROM symbols WHERE symbol = 'SPY';
```

### Cleanup Job Not Running

```bash
# Check if cron job exists
crontab -l | grep cleanup_old_data

# Check cleanup logs for errors
tail -50 /var/log/zerogex/cleanup.log

# Run cleanup manually to test
psql -h <rds-endpoint> -U postgres -d zerogex -c "SELECT * FROM cleanup_old_data();"

# Re-add cron job if missing (run Step 020 again)
./deploy/deploy.sh --start-from 020
```

### Validation Failures

If Step 200 validation fails:

```bash
# Re-run validation
./deploy/deploy.sh --start-from 200

# Check specific issues:
# - RDS connectivity
psql -h <rds-endpoint> -U postgres -d zerogex -c "SELECT NOW();"

# - Service status
sudo systemctl status zerogex-oa-ingestion
sudo systemctl status zerogex-oa-analytics

# - Python packages
cd ~/zerogex-oa
source venv/bin/activate
pip list | grep psycopg2

# - Config file permissions
ls -la ~/.pgpass ~/zerogex-oa/.env
```

### API Service Issues

**API won't start:**
```bash
# Check logs
sudo journalctl -u zerogex-oa-api -n 50

# Verify dependencies
cd ~/zerogex-oa
source venv/bin/activate
pip list | grep -E "fastapi|uvicorn|asyncpg|pydantic"

# Check if port 8000 is in use
sudo lsof -i :8000
```

**Can't access API from frontend:**
```bash
# Check firewall
sudo ufw status | grep 8000

# Open port if needed
sudo ufw allow 8000/tcp

# Test locally
curl http://localhost:8000/api/health

# For remote access, check AWS security group allows port 8000
```

**API returns no data:**
```bash
# Verify ingestion and analytics are running
sudo systemctl status zerogex-oa-ingestion
sudo systemctl status zerogex-oa-analytics

# Check database has data
make stats

# API returns 404 if no data available (normal during non-market hours)
```

## Directory Structure

```
/home/ubuntu/
├── zerogex-oa/              # Application code
│   ├── venv/                # Python virtual environment
│   ├── .env                 # Configuration
│   ├── src/                 # Source code
│   │   ├── ingestion/       # Data ingestion engine
│   │   ├── analytics/       # GEX analytics engine
│   │   └── api/             # FastAPI backend (NEW)
│   └── setup/               # Setup files
├── .pgpass                  # PostgreSQL password file (RDS credentials)
└── logs/                    # Deployment logs

/etc/systemd/system/
├── zerogex-oa-ingestion.service
├── zerogex-oa-analytics.service
└── zerogex-oa-api.service   # NEW
```

## Security Notes

1. **RDS Security**
   - Use RDS security groups to restrict database access to EC2 instances only
   - Never expose RDS to 0.0.0.0/0 (public internet)
   - Consider using AWS Secrets Manager for credentials (future enhancement)
   - Enable encryption at rest and in transit

2. **EC2 Security**
   - SSH access is key-based only (password auth disabled)
   - Firewall allows only SSH (port 22) by default
   - fail2ban protects against brute-force attacks
   - Automatic security updates enabled

3. **Credentials**
   - `.pgpass` file has 600 permissions (owner read/write only)
   - `.env` file has 600 permissions
   - TradeStation tokens stored securely in `.env`
   - Validation checks confirm secure permissions

## API Integration

### Connecting Frontend to API

**1. Configure Frontend Environment**

Create/update `frontend/.env.local`:

```bash
# API Configuration
NEXT_PUBLIC_API_URL=http://your-ec2-ip:8000
NEXT_PUBLIC_WS_URL=ws://your-ec2-ip:8000/ws

# For production with domain
NEXT_PUBLIC_API_URL=https://api.zerogex.com
NEXT_PUBLIC_WS_URL=wss://api.zerogex.com/ws

# Feature Flags
NEXT_PUBLIC_ENABLE_WEBSOCKET=true
NEXT_PUBLIC_REFRESH_INTERVAL=1000
NEXT_PUBLIC_ENV=production
```

**2. Test Connection**

```javascript
// Test API connectivity
const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/health`);
const health = await response.json();
console.log('API Status:', health.status);
console.log('Database:', health.database_connected ? 'Connected' : 'Disconnected');
console.log('Data Age:', health.data_age_seconds, 'seconds');
```

**3. Security Considerations**

For production deployment:

- **Firewall:** Restrict port 8000 to frontend IP only
- **CORS:** Update `src/api/main.py` to allow only your frontend domain
- **SSL/HTTPS:** Set up Nginx reverse proxy with Let's Encrypt SSL certificate
- **Rate Limiting:** Consider adding rate limiting for production
- **Authentication:** Add API keys or JWT tokens if needed

**Example Nginx Configuration for SSL:**

```nginx
server {
    listen 443 ssl;
    server_name api.zerogex.com;

    ssl_certificate /etc/letsencrypt/live/api.zerogex.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.zerogex.com/privkey.pem;

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}

server {
    listen 80;
    server_name api.zerogex.com;
    return 301 https://$server_name$request_uri;
}
```

## AWS RDS Best Practices

### Connection Pooling
For production, consider implementing connection pooling to reduce RDS connection overhead.

### Monitoring
Enable RDS Enhanced Monitoring and CloudWatch alarms for:
- High CPU utilization (>80%)
- Low free storage space (<10GB)
- High database connections (>80% of max)
- Read/Write IOPS approaching limits

### Backups
- RDS automated backups are enabled by default (7-day retention)
- Consider point-in-time recovery (PITR) for production
- Test restore procedures regularly
- Enable backup retention for 30+ days for production

### Performance Tuning
RDS parameter groups can be tuned for time-series workloads:
- `shared_buffers`: 25% of available RAM
- `work_mem`: 50-100MB per connection
- `maintenance_work_mem`: 512MB-2GB
- `effective_cache_size`: 75% of available RAM
- `random_page_cost`: 1.1 (for SSD storage)

### High Availability
- Consider Multi-AZ deployment for production
- Set up read replicas for reporting/analytics queries
- Configure automatic failover

## Performance Considerations

### Real-Time Views
The schema uses **regular views** (not materialized) for real-time data access with zero lag:

**Core Data Views:**
- `underlying_quotes_with_deltas` - Volume deltas for underlying
- `option_chains_with_deltas` - Volume and OI deltas for options

**Option Flow Views:**
- `option_flow_by_type` - Puts vs Calls aggregated
- `option_flow_by_strike` - Flow by strike level
- `option_flow_by_expiration` - Flow by expiration
- `option_flow_smart_money` - Unusual activity detection
- `underlying_buying_pressure` - Directional flow

**Day Trading Views:**
- `underlying_vwap_deviation` - Mean reversion signals
- `opening_range_breakout` - ORB tracking
- `gamma_exposure_levels` - Support/resistance from GEX
- `dealer_hedging_pressure` - Dealer flow amplification
- `unusual_volume_spikes` - Volume anomaly detection
- `momentum_divergence` - Price vs option flow divergence

These compute results on-the-fly using window functions, so no refresh needed.

### Indexes
The schema includes optimized indexes for:
- Time-based queries (most common pattern)
- Strike/expiration filtering
- Volume and gamma filtering
- VWAP calculations
- Opening range queries
- Recent data access patterns

### Query Performance
- Most queries are optimized for the last 1-5 minutes of data
- Indexes support date-partitioned queries
- Window functions used efficiently in views
- Consider adding custom indexes for specific query patterns

### API Performance

The API uses:
- **Async/await** for non-blocking operations
- **Connection pooling** (2-10 concurrent connections)
- **Optimized queries** leveraging database indexes
- **Direct view access** for zero-lag real-time data

**Response Times (typical):**
- Health check: <10ms
- GEX summary: 20-50ms
- GEX by strike: 30-80ms
- Flow views: 40-100ms
- Day trading views: 50-150ms

**Optimization Tips:**
- Use appropriate `limit` parameters
- Cache responses on frontend (1-5 seconds)
- Use `window_minutes` to filter data
- Consider WebSocket for true real-time (future)

## Support

For issues or questions:
1. Check deployment logs: `~/logs/deployment_*.log`
2. Review service logs via Makefile commands
3. Verify RDS security group settings
4. Check `.env` and `.pgpass` configuration
5. Run validation: `./deploy/deploy.sh --start-from 200`

## License

MIT License - See LICENSE file for details
