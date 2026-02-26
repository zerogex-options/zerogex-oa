# ZeroGEX API Endpoints Reference

Complete reference for all available API endpoints.

Base URL: `http://your-server:8000`

---

## Health & Status

### GET /api/health
Check API and database health

**Response:**
```json
{
  "status": "healthy",
  "database_connected": true,
  "last_data_update": "2026-02-26T10:30:00-05:00",
  "data_age_seconds": 45
}
```

---

## GEX (Gamma Exposure) Endpoints

### GET /api/gex/summary
Get latest GEX summary with key metrics

**Parameters:**
- `symbol` (optional): Default "SPY"

**Response:**
```json
{
  "timestamp": "2026-02-26T10:30:00-05:00",
  "symbol": "SPY",
  "spot_price": 585.42,
  "total_call_gex": 125000000,
  "total_put_gex": -95000000,
  "net_gex": 30000000,
  "gamma_flip": 580.00,
  "max_pain": 582.00,
  "call_wall": 590.00,
  "put_wall": 575.00,
  "total_call_oi": 500000,
  "total_put_oi": 450000,
  "put_call_ratio": 0.90
}
```

### GET /api/gex/by-strike
Get GEX breakdown by individual strikes

**Parameters:**
- `symbol` (optional): Default "SPY"
- `limit` (optional): Max 200, default 50

**Response:**
```json
[
  {
    "timestamp": "2026-02-26T10:30:00-05:00",
    "symbol": "SPY",
    "strike": 585.00,
    "call_oi": 25000,
    "put_oi": 18000,
    "call_volume": 5000,
    "put_volume": 3200,
    "call_gex": 5000000,
    "put_gex": -3500000,
    "net_gex": 1500000,
    "spot_price": 585.42,
    "distance_from_spot": -0.42
  }
]
```

### GET /api/gex/historical
Get historical GEX data

**Parameters:**
- `symbol` (optional): Default "SPY"
- `start_date` (optional): ISO format
- `end_date` (optional): ISO format
- `limit` (optional): Max 1000, default 100

---

## Options Flow Endpoints

### GET /api/flow/by-type
Get option flow by type (calls vs puts)

**Parameters:**
- `symbol` (optional): Default "SPY"
- `window_minutes` (optional): Max 1440, default 60

**Response:**
```json
[
  {
    "time_window_start": "2026-02-26T10:00:00-05:00",
    "time_window_end": "2026-02-26T10:30:00-05:00",
    "symbol": "SPY",
    "option_type": "CALL",
    "total_volume": 15000,
    "total_premium": 2500000,
    "avg_iv": null,
    "net_delta": 5000,
    "sentiment": "bullish"
  }
]
```

### GET /api/flow/by-strike
Get option flow by strike level

**Parameters:**
- `symbol` (optional): Default "SPY"
- `window_minutes` (optional): Max 1440, default 60
- `limit` (optional): Max 100, default 20

### GET /api/flow/smart-money
Get unusual activity / smart money flow

**Parameters:**
- `symbol` (optional): Default "SPY"
- `window_minutes` (optional): Max 1440, default 60
- `limit` (optional): Max 50, default 10

**Response:**
```json
[
  {
    "time_window_start": "2026-02-26T10:25:00-05:00",
    "time_window_end": "2026-02-26T10:30:00-05:00",
    "symbol": "SPY",
    "option_type": "C",
    "strike": 590.00,
    "total_volume": 500,
    "total_premium": 250000,
    "avg_iv": 0.18,
    "unusual_activity_score": 8,
    "size_class": "📦 Large Block",
    "notional_class": "💵 $250K+",
    "moneyness": "🎯 OTM"
  }
]
```

---

## Day Trading Endpoints

### GET /api/trading/vwap-deviation
Get VWAP deviation for mean reversion signals

**Parameters:**
- `symbol` (optional): Default "SPY"
- `limit` (optional): Max 100, default 20

**Response:**
```json
[
  {
    "time_et": "2026-02-26T10:30:00-05:00",
    "timestamp": "2026-02-26T15:30:00+00:00",
    "symbol": "SPY",
    "price": 585.42,
    "vwap": 584.98,
    "vwap_deviation_pct": 0.08,
    "volume": 15000,
    "vwap_position": "✅ Above VWAP"
  }
]
```

**Use Case:** Price >0.2% from VWAP often reverts

### GET /api/trading/opening-range
Get opening range breakout status

**Parameters:**
- `symbol` (optional): Default "SPY"
- `limit` (optional): Max 100, default 20

**Response:**
```json
[
  {
    "time_et": "2026-02-26T10:30:00-05:00",
    "timestamp": "2026-02-26T15:30:00+00:00",
    "symbol": "SPY",
    "current_price": 585.42,
    "orb_high": 585.20,
    "orb_low": 583.50,
    "orb_range": 1.70,
    "distance_above_orb_high": 0.22,
    "distance_below_orb_low": 1.92,
    "orb_pct": 112.9,
    "orb_status": "🚀 ORB Breakout (Long)",
    "volume": 15000
  }
]
```

**Use Case:** ORB breakouts often lead to trend days

### GET /api/trading/gamma-levels
Get gamma exposure levels (support/resistance)

**Parameters:**
- `symbol` (optional): Default "SPY"
- `limit` (optional): Max 100, default 20

**Response:**
```json
[
  {
    "symbol": "SPY",
    "strike": 585.00,
    "net_gex": 5000000,
    "total_gex": 8500000,
    "call_gex": 5000000,
    "put_gex": 3500000,
    "num_contracts": 25,
    "total_oi": 43000,
    "gex_level": "✅ Support Level"
  }
]
```

**Use Case:** Large positive GEX = support, negative = resistance

### GET /api/trading/dealer-hedging
Get dealer hedging pressure

**Parameters:**
- `symbol` (optional): Default "SPY"
- `limit` (optional): Max 100, default 20

**Response:**
```json
[
  {
    "time_et": "2026-02-26T10:30:00-05:00",
    "timestamp": "2026-02-26T15:30:00+00:00",
    "symbol": "SPY",
    "current_price": 585.42,
    "price_change": 0.25,
    "expected_hedge_shares": 75000,
    "hedge_pressure": "✅ Dealer Buying Pressure"
  }
]
```

**Use Case:** Amplifies moves when dealers chase price

### GET /api/trading/volume-spikes
Get unusual volume spikes

**Parameters:**
- `symbol` (optional): Default "SPY"
- `limit` (optional): Max 100, default 20

**Response:**
```json
[
  {
    "time_et": "2026-02-26T10:30:00-05:00",
    "timestamp": "2026-02-26T15:30:00+00:00",
    "symbol": "SPY",
    "price": 585.42,
    "current_volume": 45000,
    "avg_volume": 15000,
    "volume_sigma": 3.2,
    "volume_ratio": 3.0,
    "buying_pressure_pct": 65.5,
    "volume_class": "🔥 Extreme Volume Spike"
  }
]
```

**Use Case:** Volume >2 sigma often precedes big moves

### GET /api/trading/momentum-divergence
Get momentum divergence signals

**Parameters:**
- `symbol` (optional): Default "SPY"
- `limit` (optional): Max 100, default 20

**Response:**
```json
[
  {
    "time_et": "2026-02-26T10:30:00-05:00",
    "timestamp": "2026-02-26T15:30:00+00:00",
    "symbol": "SPY",
    "price": 585.42,
    "price_change_5min": 0.25,
    "net_volume": 5000,
    "net_option_flow": -75000,
    "divergence_signal": "🚨 Bearish Divergence (Price Up, Puts Buying)"
  }
]
```

**Use Case:** Divergences often precede reversals

---

## Market Data Endpoints

### GET /api/market/quote
Get current underlying quote

**Parameters:**
- `symbol` (optional): Default "SPY"

**Response:**
```json
{
  "timestamp": "2026-02-26T15:30:00+00:00",
  "symbol": "SPY",
  "open": 584.50,
  "high": 586.20,
  "low": 583.50,
  "close": 585.42,
  "volume": 2500000
}
```

### GET /api/market/historical
Get historical quotes

**Parameters:**
- `symbol` (optional): Default "SPY"
- `start_date` (optional): ISO format
- `end_date` (optional): ISO format
- `limit` (optional): Max 1000, default 100

---

## Error Responses

All endpoints return standard HTTP status codes:

- `200` - Success
- `400` - Bad Request (invalid parameters)
- `404` - Not Found (no data available)
- `500` - Internal Server Error
- `503` - Service Unavailable (database down)

**Error Response Format:**
```json
{
  "detail": "Error message here"
}
```

---

## Interactive API Documentation

FastAPI provides automatic interactive documentation:

- **Swagger UI:** `http://your-server:8000/docs`
- **ReDoc:** `http://your-server:8000/redoc`

These interfaces allow you to:
- Browse all endpoints
- See request/response schemas
- Test endpoints directly in the browser
- Download OpenAPI specification

---

## Rate Limiting

No rate limiting is currently implemented, but consider adding it for production:

```python
# Example with slowapi
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

@app.get("/api/gex/summary")
@limiter.limit("10/minute")
async def get_gex_summary():
    ...
```

---

## CORS Configuration

Currently configured to allow all origins (`*`). For production, update in `main.py`:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://your-frontend-domain.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

---

## Testing Examples

### Using curl

```bash
# Health check
curl http://localhost:8000/api/health | jq

# GEX summary
curl http://localhost:8000/api/gex/summary | jq

# Options flow with custom window
curl "http://localhost:8000/api/flow/by-type?symbol=SPY&window_minutes=30" | jq

# Smart money flow
curl "http://localhost:8000/api/flow/smart-money?limit=5" | jq

# VWAP deviation
curl http://localhost:8000/api/trading/vwap-deviation | jq
```

### Using Python

```python
import requests

# Health check
response = requests.get('http://localhost:8000/api/health')
print(response.json())

# GEX summary
response = requests.get('http://localhost:8000/api/gex/summary')
data = response.json()
print(f"Net GEX: ${data['net_gex']:,.0f}")

# Historical data with date range
response = requests.get(
    'http://localhost:8000/api/gex/historical',
    params={
        'symbol': 'SPY',
        'start_date': '2026-02-25T00:00:00',
        'end_date': '2026-02-26T23:59:59',
        'limit': 100
    }
)
historical = response.json()
```

### Using JavaScript/Fetch

```javascript
// Health check
const health = await fetch('http://localhost:8000/api/health')
    .then(res => res.json());
console.log(health);

// GEX summary
const gex = await fetch('http://localhost:8000/api/gex/summary')
    .then(res => res.json());
console.log(`Net GEX: $${gex.net_gex.toLocaleString()}`);

// Smart money flow
const smartMoney = await fetch(
    'http://localhost:8000/api/flow/smart-money?limit=10'
).then(res => res.json());
smartMoney.forEach(trade => {
    console.log(`${trade.option_type} ${trade.strike}: ${trade.unusual_activity_score}/10`);
});
```

---

## Performance Tips

1. **Use appropriate limits** - Don't fetch more data than needed
2. **Cache responses** - Especially for slower-changing data like GEX
3. **Use websockets** (future) - For real-time updates instead of polling
4. **Filter by time windows** - Use `window_minutes` parameter effectively
5. **Database indexes** - All views are optimized with proper indexes

---

## Common Patterns

### Dashboard Real-Time Updates

```javascript
// Poll every 1 second during market hours
async function updateDashboard() {
    const [gex, flow, quote] = await Promise.all([
        fetch('/api/gex/summary').then(r => r.json()),
        fetch('/api/flow/by-type?window_minutes=5').then(r => r.json()),
        fetch('/api/market/quote').then(r => r.json())
    ]);
    
    // Update UI
    updateGEXDisplay(gex);
    updateFlowChart(flow);
    updatePriceDisplay(quote);
}

setInterval(updateDashboard, 1000);
```

### Intraday Trading Signals

```python
import requests
from datetime import datetime

def check_trading_signals():
    # Check for ORB breakout
    orb = requests.get('http://localhost:8000/api/trading/opening-range').json()
    if orb and orb[0]['orb_status'].startswith('🚀'):
        print(f"ORB BREAKOUT: {orb[0]['current_price']}")
    
    # Check for volume spike
    volume = requests.get('http://localhost:8000/api/trading/volume-spikes').json()
    if volume and volume[0]['volume_sigma'] > 2:
        print(f"VOLUME SPIKE: {volume[0]['volume_sigma']}σ")
    
    # Check for divergence
    div = requests.get('http://localhost:8000/api/trading/momentum-divergence').json()
    if div and '🚨' in div[0]['divergence_signal']:
        print(f"DIVERGENCE: {div[0]['divergence_signal']}")

# Run every 30 seconds
import time
while True:
    check_trading_signals()
    time.sleep(30)
```
