# ZeroGEX-OA Deployment Guide

Complete deployment automation for the ZeroGEX Options Analytics platform on a fresh Ubuntu server.

## Overview

This deployment system automates the complete setup of ZeroGEX-OA including:
- System configuration and package installation
- PostgreSQL + TimescaleDB database setup
- Python application and dependencies
- TradeStation API integration
- Systemd service configuration
- Security hardening
- Automated backups
- Monitoring dashboard

## Prerequisites

- Ubuntu 20.04 or 22.04 LTS server
- Sudo access
- At least 4GB RAM (8GB+ recommended)
- 50GB+ storage (100GB+ recommended for production)
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
- Installs essential tools (git, curl, python3, postgresql, etc.)
- Configures timezone to America/New_York
- Sets up .bashrc with ZeroGEX environment

### Step 015: Data Volume Setup
- Detects and mounts secondary storage volumes
- Creates `/data` mount point for PostgreSQL, backups, and monitoring
- Falls back to root volume if no secondary volume available

### Step 020: Database Setup
- Installs PostgreSQL and TimescaleDB extension
- Moves PostgreSQL data directory to `/data/postgresql` (if available)
- Creates `zerogex` database and `zerogex_user`
- Applies database schema from `setup/database/schema.sql`
- Creates `.pgpass` file for passwordless access

### Step 021: Database Tuning
- Optimizes PostgreSQL for time-series workloads
- Configures connection limits and timeouts
- Sets up query logging for slow queries
- Tunes autovacuum for better performance

### Step 030: Application Setup
- Creates Python virtual environment
- Installs zerogex package in editable mode
- Installs all dependencies (core + optional)
- Creates `.env` file from template

### Step 040: TradeStation Tokens
- Interactive script to obtain TradeStation OAuth tokens
- Guides through authorization code flow
- Saves refresh token to `.env` file

### Step 050: Security Hardening
- Configures UFW firewall (SSH only)
- Hardens SSH (disables root login, password auth)
- Sets up fail2ban for brute-force protection
- Enables automatic security updates

### Step 060: Automated Backups
- Installs database backup script
- Configures daily backups via cron (2:00 AM)
- Sets 7-day retention policy
- Stores backups in `/data/backups`

### Step 070: Systemd Services
- Installs systemd service files:
  - `zerogex-oa-ingestion.service` - Data ingestion engine
  - `zerogex-oa-analytics.service` - GEX analytics engine
- Enables services to start on boot
- Starts services and verifies status

### Step 080: Validation
- Comprehensive deployment validation
- Checks service status, database connectivity
- Verifies schema, tables, and views
- Tests Python environment and packages
- Reports summary with pass/fail/warnings

### Step 090: Monitoring (Optional)
- Sets up monitoring dashboard on port 8080
- Collects system, service, and database metrics
- Stores metrics in `/data/monitoring`

## Partial Deployment

You can start deployment from any step:

```bash
# Start from database setup
./deploy/deploy.sh --start-from database

# Start from application setup
./deploy/deploy.sh --start-from 030

# Start from systemd services
./deploy/deploy.sh --start-from systemd
```

## Post-Deployment Configuration

### 1. Configure TradeStation API

Edit `~/zerogex-oa/.env`:

```bash
TRADESTATION_CLIENT_ID=your_client_id_here
TRADESTATION_CLIENT_SECRET=your_client_secret_here
TRADESTATION_REFRESH_TOKEN=will_be_set_by_step_040
TRADESTATION_USE_SANDBOX=false
```

### 2. Configure Database Password (if not using default)

Edit `~/.pgpass`:

```
localhost:5432:zerogex:zerogex_user:your_secure_password
```

Then update `.env` to use pgpass:

```bash
DB_PASSWORD_PROVIDER=pgpass
```

### 3. Add SPY Symbol

```bash
cd ~/zerogex-oa
source venv/bin/activate
psql -h localhost -U zerogex_user -d zerogex -c \
  "INSERT INTO symbols (symbol, name, asset_type) 
   VALUES ('SPY', 'SPDR S&P 500 ETF', 'ETF') 
   ON CONFLICT DO NOTHING;"
```

### 4. Test Data Ingestion

```bash
# Watch ingestion logs
make ingestion-logs

# Check recent data
make latest
make stats
```

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

# View logs
sudo journalctl -u zerogex-oa-ingestion -f
sudo journalctl -u zerogex-oa-analytics -f
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
# Connect to database
make psql

# Run custom query
make query SQL="SELECT COUNT(*) FROM underlying_quotes;"
```

## Monitoring

Access monitoring dashboard:
```
http://<server-ip>:8080
```

View metrics:
```bash
cat /data/monitoring/current_metrics.json | jq
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
- TradeStation credentials not configured
- Database connection issues
- Missing .pgpass file or wrong permissions

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
```

### Database Connection Errors

Verify .pgpass permissions:
```bash
chmod 600 ~/.pgpass
```

Test connection:
```bash
psql -h localhost -U zerogex_user -d zerogex -c "SELECT NOW();"
```

## Directory Structure

```
/home/ubuntu/
├── zerogex-oa/              # Application code
│   ├── venv/                # Python virtual environment
│   ├── .env                 # Configuration
│   ├── src/                 # Source code
│   └── setup/               # Setup files
├── .pgpass                  # PostgreSQL password file
└── logs/                    # Deployment logs

/data/                       # Data volume (if available)
├── postgresql/              # PostgreSQL data files
├── backups/                 # Database backups
└── monitoring/              # Monitoring metrics

/etc/systemd/system/
├── zerogex-oa-ingestion.service
└── zerogex-oa-analytics.service
```

## Security Notes

1. **Change default passwords** in `.pgpass` immediately after deployment
2. **SSH access** is key-based only (password auth disabled)
3. **Firewall** allows only SSH (port 22) and monitoring (port 8080)
4. **fail2ban** protects against brute-force attacks
5. **Automatic updates** enabled for security patches

## Backup and Recovery

### Manual Backup

```bash
# Run backup now
sudo -u ubuntu /usr/local/bin/backup-zerogex-db.sh
```

### List Backups

```bash
ls -lh /data/backups/
```

### Restore from Backup

```bash
pg_restore -h localhost -U zerogex_user -d zerogex /data/backups/zerogex_YYYYMMDD_HHMMSS.dump
```

## Performance Tuning

PostgreSQL is pre-tuned for time-series workloads, but you can adjust:

```bash
# Edit PostgreSQL config
sudo vim /etc/postgresql/*/main/postgresql.conf

# Restart PostgreSQL
sudo systemctl restart postgresql
```

## Support

For issues or questions:
1. Check deployment logs: `~/logs/deployment_*.log`
2. Review service logs via Makefile commands
3. Verify configuration in `.env` and `.pgpass`

## License

MIT License - See LICENSE file for details
