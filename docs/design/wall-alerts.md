# Wall alerts

**Status:** shipped. Detector `src/alerts/wall_detector.py`, service
`src/alerts/wall_alert_engine.py` (unit `zerogex-oa-wall-alerts`), API
`src/api/routers/wall_alerts.py`, tables `wall_alert_*` in `schema.sql`.
Website: `/account/alerts`, the header bell, and the
`zerogex-web-wall-alerts-notify` email worker.

**What a member gets:** "tell me when the SPX put wall is hit — but only when
it is a strong wall, without the chop — in the app and by email." Three things
are configurable per alert: which events, how strong a wall must be, and how
hard noise is filtered.

---

## 1. Events: a test, not a crossing

A big strike is a magnet and a barrier at once, so price spends long stretches
oscillating across it. An alert that fires on every crossing fires dozens of
times a session, and every one of those crossings is the same decision for a
trader. So the unit is a **test** of a wall — the same unit
`src/analytics/wall_breaks.py` labels for the published break odds — and each
test produces at most two alerts:

| Event | Meaning | Fires on |
|---|---|---|
| **Hit** | price reached the wall | the running high (call) / low (put) of the minute still forming entering the touch band — seconds after the print |
| **Broke** | the wall gave way | `confirm_minutes` consecutive 1-minute closes beyond the wall by the break buffer; the run resets the moment one close falls back inside |
| **Held** | price was turned away | a close back off the wall by the hold distance before a break confirms |

A test that resolves neither way within `resolution_minutes` lapses silently.
A broken wall is **spent** for the session on that side (as in the research).

## 2. The noise controls

| Control | What it stops |
|---|---|
| **Arming.** A wall alerts a hit only after a minute closed at least the *arm distance* away on its approach side. | Forty minutes pressed against a wall being forty alerts. |
| **Cooldown.** At most one hit alert per wall per `cooldown_minutes`. | A clean back-off-and-return every few minutes. |
| **Volatility-scaled distances.** Arm, hold and break distances are multiples of the platform's break buffer — `max(0.1% of spot, 40% of the typical 30-min range)`, the same yardstick that decides when the chart moves a strike to the other side. | Fixed cents reading the same on a quiet morning and a fast afternoon. |
| **Confirmed breaks.** N consecutive closes past the buffer. | Failed breakouts, which "often hold for the first ten or fifteen minutes before unwinding". |
| **Re-picked walls start disarmed.** | A relabel onto the strike price already sits at being reported as an arrival. |
| **Open grace.** | The opening auction being read as a test. |
| **Closes, not wicks, for outcomes and arming.** | One wick confirming a break or re-arming a wall. |
| **Daily cap per member** (`WALL_ALERTS_DAILY_CAP_PER_USER`, 60). | A flood on an abnormal day, whatever goes wrong upstream. |

Two rules decide which outcomes are alerted, and both are member-legible:

* **Every break is alerted** — even for a test whose hit was quiet (cooldown,
  open grace, or a wall that chopped all morning and never armed). A break is
  confirmed and cannot chop, and it is the one alert that must never be
  swallowed.
* **A hold is alerted only after an alerted hit.** "Held" with no "hit" before
  it reads as nonsense, and a quiet period covers the bounce that follows it.

Every touch of a published wall opens a test whether or not the wall is armed —
arming only decides whether the *hit* is alerted. That is what lets a pinned
wall that finally gives way at 2 PM still alert its break.

## 3. Sensitivity profiles

Members pick one of three; the numbers behind each stay coherent with each
other. Distances are in break buffers (≈ 7–8 points on SPX, ≈ 0.8 on SPY on a
normal day).

| | Responsive | **Balanced** (default) | Conservative |
|---|---|---|---|
| Touch band | 8 bp | 5 bp (`level_history.TOUCH_PCT`) | 3 bp |
| Arm distance | 1.0 × buffer | 1.5 × | 2.0 × |
| Cooldown | 10 min | 20 min | 30 min |
| Break confirmation | 3 closes | 5 closes | 10 closes (`MIN_HOLD_MINUTES`) |
| Hold distance | 1.0 × buffer | 1.5 × | 2.0 × |
| Test resolution | 45 min | 60 min | 60 min |
| Open grace | none | 3 min | 10 min |

All three profiles run for every symbol whether or not anyone subscribes, so
`wall_alert_events` is a complete record of what each would have sent — the
data to retune them against. `make wall-alerts-replay SYMBOL=SPX DATE=…` runs
the detectors over any stored session offline (read-only) and prints each
profile's alerts.

## 4. Wall strength

"How strong a wall" is the wall's dollar gamma as a **percentile of the
symbol's own 30-day history at the same 5-minute time of day**
(`gex_historical_stats`, refreshed nightly; `gex_stats.fetch_wall_strength_pctile`).
Symbol-relative, because SPX runs an order of magnitude more dollar gamma than
SPY; time-of-day-relative, because 0DTE gamma at 15:55 dwarfs 09:35 and a flat
comparison would call every afternoon wall "huge". So "Strong" means the same
thing on every symbol at every hour.

Tiers (the slider's stops, also on every alert): Light < 25 ≤ Moderate < 50 ≤
Above typical < 75 ≤ Strong < 90 ≤ Very strong. The strength used is the
wall's at the hit, and the test's outcome alerts inherit it.

A rule with a floor above 0 only matches a **known** percentile: "only strong
walls" is never satisfied by "we could not tell" (a symbol with no history
yet, or a nightly refresh that has not run).

## 5. Data model

| Table | Holds |
|---|---|
| `wall_alert_rules` | Each member's alerts: symbol, side, events, strength floor, profile, channels, enabled. |
| `wall_alert_events` | Every event, unique on `(symbol, session, profile, side, wall, test, kind)`, so re-running a tape is a no-op. `payload` is everything a channel renders (`wall_payload.py`). |
| `wall_alert_deliveries` | The outbox, unique per `(event, member, channel)`. `in_app` rows are born `sent` (the feed is the delivery, with server-side `read_at`); `email` / `sms` rows are born `queued`. |
| `wall_alert_detector_state` | Per-(symbol, profile) detector state, rewritten when it changes (≥ once a minute in session — the heartbeat). |

An event and its deliveries are written in one transaction, and the detector's
in-memory state is rewound if that write fails, so an alert is never recorded
without its deliveries, never lost, and never sent twice.

## 6. Channels

`src/alerts/config.py CHANNELS` registers each channel as `instant` (written
`sent`) or `queued` (drained by a worker through `/api/alerts/walls/internal/*`).

* **In-app** — the website's header bell polls `/feed`; a toast (and, if the
  member allows it, a desktop notification) announces new rows.
* **Email** — `frontend/scripts/wall-alerts-deliver.mts` on the website box,
  every 20 seconds: groups each member's queued rows into one message (SPX and
  SPY walls often fire together), checks the member is verified, not deleted
  and still Basic or above, sends through Resend with one-click unsubscribe,
  and marks the rows. Rows older than 20 minutes expire unsent — a wall alert
  is only worth sending while it is news.

The queue endpoints re-check opt-in at delivery time: a row whose rule was
deleted, disabled or had the channel switched off is cancelled, never sent.
They require `X-Admin-Token` because the API key alone is held by every
external integrator and the website BFF forwards browser calls with its own.

### Wiring SMS

Everything but the last mile is in place: rules, the outbox and the internal
endpoints already speak `sms`, and the website renders an SMS-length message
from the same payload (`wallAlertSmsText` in `frontend/core/wallAlerts.ts`,
≤ 160 characters). To ship it:

1. **Phone numbers.** Add `phone_e164` + `phone_verified_at` to the website's
   `users` table and a verify-by-code flow on `/account/alerts`. Never send to
   an unverified number.
2. **Consent.** Record explicit SMS opt-in (TCPA / CTIA): the checkbox copy,
   timestamp and IP. Honor STOP / HELP replies from the provider's webhook by
   calling `POST /api/alerts/walls/internal/disable-channel {channel: "sms"}`.
3. **Provider.** Twilio (or similar) with a registered A2P 10DLC campaign or a
   toll-free verified number — unregistered US traffic is filtered by carriers.
4. **Worker.** `wall-alerts-deliver.mts --channel sms`: the same loop as email
   with an SMS adapter (resolve the verified phone, send `wallAlertSmsText`,
   `mark`). One message per event; do not batch SMS.
5. **Switch on** `WALL_ALERTS_SMS_ENABLED=true` on the API box **last**. Until
   then the API refuses SMS rules and the fan-out queues nothing for SMS.

## 7. Operations

* Install: `make schema-apply` (tables first), then `make wall-alerts-install`,
  then restart the API so the router loads.
* Health: `make wall-alerts-health` — service state, per-symbol heartbeat,
  today's events per profile, the outbox by status, rule counts.
* After pulling a change under `src/alerts/`: `make wall-alerts-restart` (the
  bulk `services-restart` does not touch this unit).
* Kill switch: `WALL_ALERTS_ENABLED=false` + restart. Rules are untouched.
* Freshness: a symbol whose tape or walls stopped updating is skipped (logged
  once as `skipping SPX — walls stale`), and an event found while catching up
  after a gap is applied to state but not delivered.
* Restart-safe: state restores from `wall_alert_detector_state`; with none, the
  service warms up from the last 90 minutes of tape with delivery suppressed.
* One instance: an advisory lock; a second copy stands by.
* SQL harness: `make wall-alerts-db-test WALL_ALERTS_TEST_DSN=…` (scratch DB).

## 8. Deliberately not done

* **ES / NQ.** Their walls are SPX / NDX carried onto the futures axis at fair
  value, so an "ES hit" would be a statement about the carry model as much as
  the wall. Members set the alert on the index; the form says so.
* **Extended hours.** Regular session only. Cash indexes do not print outside
  it, and the walls are least reliable there.
* **Secondary walls (C2 / P2).** The data model keys on the strike, so adding
  ranks is a detector input change, not a schema change.
