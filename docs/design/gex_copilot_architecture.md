# GEX Copilot — Architecture & Specification

**Status:** Draft (PR-1 of the Copilot stream)
**Branch:** `claude/friendly-johnson-4clwgc`
**Owner:** ZeroGEX core
**Depends on:** existing Playbook Engine (`docs/playbook_catalog.md`), MSI regime classifier, signals catalog, GEX/flow analytics

---

## 0. One-paragraph thesis

ZeroGEX already computes quant-grade dealer-positioning data and emits structured `ActionCard`s through the Playbook Engine. The remaining gap for novice traders is **interpretation, not information**: they cannot translate "net GEX -$2.3B, flip at 5847, vanna-charm flow positive" into "do I buy, sell, or sit out, and why." The Copilot is a thin, deterministic narrative layer plus a grounded LLM chat agent that sits on top of the existing engines. **Every sentence the agent speaks is bound to a row returned by an internal tool call. No free-form speculation.** This gives ZeroGEX something nobody else has — a hallucination-resistant trading mentor — because the substrate is structured, not scraped.

---

## 1. Scope & non-goals

### In scope (this design)

1. **Regime Narrative Classifier** — a deterministic translator from the existing MSI regime + GEX posture into one of five *novice-comprehensible* regime narratives, with expected behavior, what-to-avoid, and confidence.
2. **Novice Card** — a thin wrapper around the existing `ActionCard` that adds plain-English fields a novice needs (dollar risk, what-could-go-wrong, invalidation, paper-trade lifecycle hooks). The internal trade mechanics stay in `ActionCard` unchanged.
3. **Chat-Grounding Tool Contract** — the exact set of internal endpoints the LLM may call, the input/output schema for each, and the system-prompt invariants that prevent speculation.
4. **Storage & API surface** — one new table, three new endpoints. Nothing else changes.
5. **MVP rollout plan** — 6-week shippable.

### Out of scope (defer)

- Daily Weather Report video generation (separate stream — uses these primitives but generated artifacts live in `zerogex-web`).
- Real-time audio narration stream.
- "Analog Replay" simulator.
- Broker integration / live order routing.
- Auto-execution. Cards are advisory.

### What this is **not**

- Not a replacement for the Playbook Engine. Cards still originate there.
- Not a new regime classifier. MSI remains the source of truth; we *re-label* its output for novice comprehension.
- Not a black box. Every Copilot statement must point to a structured source.

---

## 2. Architecture

```
                ┌───────────────────────────────────────────┐
                │       Existing ZeroGEX substrate          │
                │  (unchanged — analytics + signals +       │
                │   playbook engine + DB)                   │
                └────────────────┬──────────────────────────┘
                                 │
        ┌────────────────────────┴────────────────────────┐
        │                                                  │
        ▼                                                  ▼
┌─────────────────────┐                       ┌─────────────────────┐
│  Regime Narrative   │                       │     Novice Card     │
│     Classifier      │                       │  (wraps ActionCard) │
│                     │                       │                     │
│  MSI regime + GEX   │                       │  ActionCard + plain │
│  + signals  →       │                       │  English + dollar   │
│  Novice regime tag  │                       │  risk + invalidation│
└──────────┬──────────┘                       └──────────┬──────────┘
           │                                              │
           └──────────────────┬───────────────────────────┘
                              ▼
              ┌───────────────────────────────────┐
              │       Chat-Grounding tools        │
              │  (the LLM's only knobs)           │
              └────────────────┬──────────────────┘
                               ▼
              ┌───────────────────────────────────┐
              │  Copilot Chat Agent (LLM)         │
              │  System prompt: tool-grounded     │
              │  Disallows speculation            │
              └───────────────────────────────────┘
```

**Why this layering matters:** the Copilot can be lifted out at any time without touching analytics. The LLM is a thin shell; the deterministic layers are the moat.

---

## 3. Regime Narrative Classifier — spec

### 3.1 Purpose

Translate the existing internal MSI regime (`trend_expansion` / `controlled_trend` / `chop_range` / `high_risk_reversal`) and GEX posture into one of five *novice-comprehensible* labels, with a confidence score and plain-English narration. Stable across time (hysteresis), auditable (carries its input snapshot), and queryable historically (for the analog-replay feature in v2).

### 3.2 Novice regime labels

| Label | Plain-English | What to expect | What novices should avoid |
|---|---|---|---|
| `LONG_GAMMA_PIN` | Dealers are long gamma; price gets pulled toward a magnet level | Chop, mean-reversion, low realized vol, pin into close | Chasing breakouts, holding directional 0DTEs |
| `SHORT_GAMMA_TREND` | Dealers are short gamma; their hedging amplifies moves | Trend continuation, larger ranges, vol expansion risk | Fading the move, selling naked premium |
| `VOL_EXPANSION` | Volatility is breaking out of recent regime | Wider candles, gap risk, dealer hedging chaotic | Tight stops, overnight long gamma without hedge |
| `VANNA_GLIDE` | Vol direction is dragging spot via vanna | Smooth directional drift, low realized vs implied | Fading the drift, mean-reversion plays |
| `CHARM_DRIFT` | Time decay is pinning spot toward max pain into close | Slow drift to OI pivot, last-hour close-direction skew | Holding 0DTE OTM hoping for a move |
| `TRANSITION` | Conditions ambiguous; regime in flux | Reduced edge across all patterns | All new entries — wait for resolution |
| `UNDEFINED` | Insufficient data | n/a | n/a |

### 3.3 Inputs

Sourced from existing fields on `PlaybookContext` and `MarketContext` — no new ingestion required.

| Input | Source | Notes |
|---|---|---|
| `net_gex` | `ctx.net_gex` | dollar gamma exposure |
| `spot` | `ctx.close` | underlying last |
| `gamma_flip` | `ctx.level("gamma_flip")` | from analytics engine |
| `max_pain` | `ctx.level("max_pain")` | from analytics engine |
| `call_wall`, `put_wall` | `ctx.level(...)` | from `analytics/walls.py` |
| `vix_level` | `ctx.market.vix_level` | from vix_ingester |
| `vix_change_pct` | derived from VIX history | day-over-day pct |
| `realized_vol_30m` | rolling stdev of 1-min closes | annualized |
| `vanna_charm_flow.score` | `ctx.basic("vanna_charm_flow")` | basic signal |
| `vol_expansion.triggered` | `ctx.advanced("vol_expansion")` | advanced signal |
| `eod_pressure.score` | `ctx.advanced("eod_pressure")` | advanced signal |
| `zero_dte_position_imbalance.score` | `ctx.advanced("zero_dte_position_imbalance")` | advanced signal |
| `tape_flow_bias.score` | `ctx.basic("tape_flow_bias")` | basic signal |
| `msi_regime` | `ctx.msi_regime` | the internal 4-tier regime |
| `et_time`, `minutes_to_close` | `ctx.et_time`, `ctx.minutes_to_close` | session phase |

### 3.4 Classification rules (deterministic, ordered)

Rules are evaluated in priority order. The **first** matching rule wins. This is intentional: a `VOL_EXPANSION` day overrides everything else.

> **Constants below are PR-1 priors.** PR-3 replaces them with empirically tuned thresholds, identical to the `pattern_base` calibration in the Playbook catalog §5.

1. **`VOL_EXPANSION`** if all hold:
   - `vol_expansion.triggered == True`
   - `vix_change_pct >= +5.0` OR `realized_vol_30m / vix_level >= 0.85` (realized catching implied)
   - `|net_gex| < 5e8` OR `net_gex < 0`

2. **`VANNA_GLIDE`** if all hold:
   - `abs(vanna_charm_flow.score) >= 60`
   - `abs(vix_change_pct) >= 2.0` (vol is *moving*, in either direction)
   - rule 1 did not match

3. **`SHORT_GAMMA_TREND`** if all hold:
   - `net_gex < -1.0e9`
   - `spot < gamma_flip` (or `spot > gamma_flip` for the bullish mirror — sign of `tape_flow_bias` resolves direction)
   - `sign(tape_flow_bias.score) == sign(spot - gamma_flip)` (tape confirms regime)
   - `msi_regime in {"trend_expansion", "controlled_trend"}`

4. **`CHARM_DRIFT`** if all hold:
   - `minutes_to_close <= 90`
   - `zero_dte_position_imbalance.score >= 60` OR `eod_pressure.score >= 60`
   - `abs(spot - max_pain) / spot <= 0.005` and converging (signed distance shrinking over last 10 minutes)

5. **`LONG_GAMMA_PIN`** if all hold:
   - `net_gex > +1.0e9`
   - `abs(spot - max_pain) / spot <= 0.003` OR `abs(spot - call_wall) / spot <= 0.003` OR `abs(spot - put_wall) / spot <= 0.003`
   - `realized_vol_30m <= 0.10` (annualized)
   - `msi_regime in {"chop_range", "high_risk_reversal"}`

6. **`TRANSITION`** if none of 1–5 matched but at least one was a near-miss (≥ 70% of its criteria true).

7. **`UNDEFINED`** otherwise, or whenever any required input is `None`.

### 3.5 Confidence score

```
confidence = base * criteria_fit
```

- `base`: 0.70 for rules 1–5, 0.40 for `TRANSITION`, 0.0 for `UNDEFINED`.
- `criteria_fit`: For the matched rule, fraction of *strict* criteria (e.g. `net_gex < -1.0e9` is strict, `tape confirms direction` is strict) that pass with margin >= 25% above their threshold. Range 0.6–1.3. Clamped.
- Final `confidence` clamped to `[0.0, 0.95]`. We never claim certainty.

### 3.6 Hysteresis

To prevent label flapping at boundaries:

- Each cycle's regime is committed only if (a) it matches the prior cycle's regime, OR (b) the prior regime's criteria no longer hold AND the new regime's confidence ≥ `prior_confidence + 0.10`.
- Otherwise emit `TRANSITION` until the threshold is crossed.

### 3.7 Output schema

```python
@dataclass(frozen=True)
class RegimeNarrative:
    timestamp: datetime
    symbol: str
    label: str              # see §3.2
    confidence: float       # [0.0, 0.95]
    spot: float
    expected_behavior: str  # plain-English, one sentence
    favored_patterns: list[str]  # playbook pattern IDs that play this regime well
    avoid: list[str]        # plain-English, list of don'ts
    what_would_flip_it: str # the single biggest input change that would change the label
    inputs_snapshot: dict[str, Any]  # audit trail — every input used
    msi_regime: str         # for cross-reference to existing engine
```

### 3.8 Storage

New TimescaleDB hypertable:

```sql
CREATE TABLE IF NOT EXISTS regime_narratives (
    ts          TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    label       TEXT NOT NULL,
    confidence  DOUBLE PRECISION NOT NULL,
    spot        DOUBLE PRECISION NOT NULL,
    msi_regime  TEXT NOT NULL,
    inputs      JSONB NOT NULL,
    PRIMARY KEY (ts, symbol)
);
SELECT create_hypertable('regime_narratives', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS regime_narratives_symbol_ts_idx
  ON regime_narratives (symbol, ts DESC);
```

Retention: 365 days (same as `signal_scores`). Chunking: 1 day.

### 3.9 Cadence

Computed once per analytics cycle (existing 30–60 s window) inside `PlaybookEngine.cycle()` after MSI scoring and signal evaluation, before pattern matching. Cost is negligible: pure function over fields already in `PlaybookContext`.

---

## 4. Novice Card — spec

### 4.1 Purpose

`NoviceCard` wraps `ActionCard` (unchanged) and adds the fields a novice needs to understand and execute the trade without the underlying Greeks knowledge. The internal Playbook Engine emits `ActionCard`s; a thin adapter (`copilot/novice_card.py`) projects them into `NoviceCard`s for the Copilot surface.

### 4.2 Why wrap, not extend

- The internal `ActionCard` is consumed by `portfolio_engine.py` and the live `/api/signals/action` endpoint. Adding novice-facing fields directly bloats the internal contract and risks coupling sizing logic to UX copy. The wrapper isolates the two audiences.

### 4.3 Schema (additions only — full schema in `copilot/novice_card.py`)

```python
@dataclass(frozen=True)
class NoviceCard:
    # Identification ----------------------------------------------------
    card_id: str                 # UUID v4, generated at emission
    action_card: ActionCard      # the underlying instruction (verbatim)
    regime: RegimeNarrative      # the regime context at emission

    # Plain-English -----------------------------------------------------
    one_line_thesis: str         # "Dealers short gamma + tape broke flip = expect continuation lower"
    what_could_go_wrong: str     # one sentence, names the main risk
    invalidation: str            # "Card invalid if spot returns above 5847 within 15 min"

    # Risk in human units (per account_size, default $10k) -------------
    account_size: float          # $10k default; user-configurable downstream
    risk_dollars: float          # max loss if stopped, including spread cost
    risk_pct_of_account: float   # default cap 1.0%
    target_dollars: float        # primary target P&L
    payoff_ratio: float          # target_dollars / risk_dollars

    # Credibility -------------------------------------------------------
    historical_hit_rate: Optional[float]    # from signal_trades over last 90d
    historical_sample_size: int             # n trades the rate is based on
    historical_avg_winner_pct: Optional[float]
    historical_avg_loser_pct: Optional[float]
    experimental: bool           # True when sample_size < 10 — UX must show caveat

    # Lifecycle ---------------------------------------------------------
    emitted_at: datetime
    expires_at: datetime         # auto-invalidate; defaults to action_card.max_hold_minutes
    status: str = "ACTIVE"       # ACTIVE | FILLED | STOPPED | TARGET_HIT | INVALIDATED | EXPIRED
```

### 4.4 Generation rules

- **One `NoviceCard` per `ActionCard`**, generated by the adapter when the Playbook Engine emits.
- **`STAND_DOWN` cards** still produce a `NoviceCard` — the `one_line_thesis` becomes "Nothing to trade right now" and `what_could_go_wrong` carries the structured near-miss list for transparency.
- **`experimental = True`** when `historical_sample_size < 10`. The UX is required to show a "Pattern still being validated" badge for these.
- **Dollar sizing** uses the smallest leg-quantity that respects `risk_pct_of_account <= 1.0%`, computed from `entry.limit_premium` and `stop.exit_premium`. If the minimum 1-contract risk already exceeds the cap, the card is downgraded to `STAND_DOWN` with rationale "Trade exceeds 1% account risk at minimum size".
- **Historical stats** are pulled from the existing `signal_trades` table (`pattern_id` + 90-day window). When `signal_trades` has no entries yet, `historical_hit_rate = None`, `experimental = True`.

### 4.5 Storage

New table:

```sql
CREATE TABLE IF NOT EXISTS novice_cards (
    card_id          UUID PRIMARY KEY,
    underlying       TEXT NOT NULL,
    pattern_id       TEXT NOT NULL,
    emitted_at       TIMESTAMPTZ NOT NULL,
    expires_at       TIMESTAMPTZ NOT NULL,
    status           TEXT NOT NULL DEFAULT 'ACTIVE',
    regime_label     TEXT NOT NULL,
    regime_confidence DOUBLE PRECISION NOT NULL,
    action_card      JSONB NOT NULL,   -- verbatim ActionCard.to_dict()
    novice_fields    JSONB NOT NULL,   -- the wrapper-only fields
    closed_at        TIMESTAMPTZ,
    realized_pnl_dollars DOUBLE PRECISION
);
CREATE INDEX novice_cards_underlying_emitted_idx ON novice_cards (underlying, emitted_at DESC);
CREATE INDEX novice_cards_status_idx ON novice_cards (status) WHERE status = 'ACTIVE';
```

### 4.6 Status lifecycle

A small background job (already-existing analytics cycle hook) updates `status` on each pass:

- `ACTIVE` → `FILLED` when underlying touches `entry.ref_price` zone within the trigger semantics
- `FILLED` → `TARGET_HIT` when `target` reached
- `FILLED` → `STOPPED` when `stop` reached
- `ACTIVE` → `INVALIDATED` when regime label changes away from the regime at emission AND new regime is in pattern's `valid_regimes` exclusion list
- any → `EXPIRED` when `expires_at` passes

`realized_pnl_dollars` is populated on terminal status.

---

## 5. Chat-Grounding Tool Contract

### 5.1 The non-negotiable invariants

The Copilot LLM operates under these system-prompt invariants. They are enforced *structurally* (the LLM has access to nothing outside the tool list) and *semantically* (the system prompt forbids unsourced claims):

1. **Every numerical claim must originate from a tool result in the current turn.** No remembered numbers from prior turns. No estimates.
2. **Every directional claim ("expect upside", "watch for reversal") must cite a `RegimeNarrative` or a `NoviceCard`.**
3. **No price predictions.** The Copilot describes regime, expected behavior, and structured trade ideas. It does not say "SPY will hit 6000".
4. **No leverage suggestions.** No "you should buy 10 contracts". Position size comes from `NoviceCard.risk_dollars`; the Copilot may only relay it.
5. **No promises.** Use "historically this pattern has ~64% hit rate over 22 fires" — never "this trade will work".
6. **STAND_DOWN is a valid answer.** If no card is ACTIVE and the user asks "what should I do", the Copilot must say "sit out, here's why" — not invent a trade.

### 5.2 Tool catalog

All tools are read-only. All return strict JSON matching the schemas in `copilot/grounding_tools.py`.

| Tool name | Purpose | Returns |
|---|---|---|
| `get_current_regime(symbol)` | Latest `RegimeNarrative` for the symbol | `RegimeNarrative` JSON |
| `get_active_cards(symbol)` | All `NoviceCard`s with status=ACTIVE | `list[NoviceCard]` |
| `get_card_by_id(card_id)` | Lookup specific card (for "what's the status of card X") | `NoviceCard` |
| `get_recent_card_history(pattern_id, days=30)` | Outcomes of recent fires of a pattern (credibility surface) | `list[ClosedCardSummary]` |
| `get_levels_snapshot(symbol)` | Current call_wall, put_wall, gamma_flip, max_pain, spot | `LevelsSnapshot` |
| `get_position_context(symbol, strike, right, expiry)` | Where this position sits relative to current levels & regime | `PositionContext` |
| `narrate_recent_changes(symbol, lookback_minutes=60)` | Regime transitions + card emissions in window | `list[Event]` |
| `get_regime_history(symbol, days=5)` | Past regime labels (analog discovery) | `list[RegimeNarrative]` |

### 5.3 System prompt (canonical)

Stored at `src/copilot/prompts/copilot_system.md`. Excerpt:

> You are ZeroGEX Copilot. You help novice options traders understand dealer positioning and act on structured trade cards. **Every factual claim you make must cite a tool you called this turn.** You may use tools freely. You may not invent numbers, predict prices, or recommend sizes beyond what the card states. If no card is active, your honest answer is "sit out". You speak in short sentences. You define jargon the first time you use it.

### 5.4 Refusal cases (built into system prompt)

- "Should I YOLO into 0DTEs?" → "No. Here's why and what the current regime supports instead." Cite regime + active cards.
- "Will SPY hit 6000 tomorrow?" → "I don't predict prices. Here's what regime + GEX levels suggest." Cite regime.
- "I'm down 50% on my position, should I average down?" → No averaging-down advice. Pull `get_position_context`, narrate exposure, suggest reviewing `invalidation` of any related card. Always recommend talking to a human if loss approaches 1% of account.

---

## 6. API surface

Three new endpoints under `/api/copilot`:

| Method | Path | Returns |
|---|---|---|
| GET | `/api/copilot/regime/{symbol}` | `RegimeNarrative` |
| GET | `/api/copilot/cards/active?symbol={symbol}` | `list[NoviceCard]` (status=ACTIVE) |
| GET | `/api/copilot/cards/history?pattern_id={id}&days={n}` | `list[ClosedCardSummary]` |

Auth: existing API key + JWT pattern. New scope: `COPILOT`. Granted by default to all paid tiers.

No changes to existing endpoints.

---

## 7. MVP rollout (6 weeks)

| Week | Deliverable | Definition of done |
|---|---|---|
| 1 | `RegimeNarrative` dataclass + classifier (rules 1–5) + `regime_narratives` table | Unit tests: 20 hand-labeled regime snapshots from `gex_summary` history correctly classified ≥ 18/20 |
| 2 | `NoviceCard` adapter + `novice_cards` table + status-lifecycle job | Replay 7 trading days of historical `ActionCard`s → wrapper produces well-formed cards 100% of time |
| 3 | API endpoints `/api/copilot/*` + tool contracts in `grounding_tools.py` | Smoke test: each tool returns valid schema; latency <200 ms p95 |
| 4 | LLM chat agent (Claude Haiku 4.5, tool-use loop, system prompt) + adversarial prompt eval | 50-prompt adversarial set: zero hallucinations, zero numerical fabrications, zero price predictions |
| 5 | Frontend chat surface in `zerogex-web` (single component, polled status) | Internal dogfood: 5 team members use for one full session, log every issue |
| 6 | Soft launch to Founding Member cohort behind feature flag | 20 invited users; collect first-value time, % who ask 3+ questions |

### Success metrics (post-launch)

- **Time-to-first-value:** median novice user reaches an `ACTIVE` card explanation in < 90 s.
- **Hallucination rate:** zero, measured by sampling 100 conversations/week.
- **Trial→paid lift:** target +3 percentage points from baseline 5.4%.
- **Novice retention:** D30 retention +10 pp vs. dashboard-only cohort.

---

## 8. What goes in `zerogex-oa` vs `zerogex-web`

| Component | Repo |
|---|---|
| Regime Narrative Classifier | `zerogex-oa/src/copilot/regime_narrative.py` |
| Novice Card adapter + types | `zerogex-oa/src/copilot/novice_card.py` |
| Tool contract schemas + handlers | `zerogex-oa/src/copilot/grounding_tools.py` |
| System prompt | `zerogex-oa/src/copilot/prompts/copilot_system.md` |
| LLM chat agent loop | `zerogex-oa/src/copilot/agent.py` |
| API routers | `zerogex-oa/src/api/routers/copilot.py` |
| Schema migrations | `zerogex-oa/setup/database/migrations/NN_copilot.sql` |
| Chat UI component | `zerogex-web/frontend/components/copilot/` |
| `useCopilot` hook | `zerogex-web/frontend/hooks/useCopilot.ts` |

---

## 9. Risks & mitigations

| Risk | Mitigation |
|---|---|
| LLM hallucination despite tool grounding | Adversarial eval set (week 4); zero-tolerance gate; any unsourced numeric claim fails the eval |
| Regime label flapping at boundaries | Hysteresis rule §3.6 |
| Novice misinterprets a STAND_DOWN as "be aggressive" | UX: STAND_DOWN cards have dedicated component that explicitly says "Sit out" |
| `signal_trades` history too thin to back claims | `experimental` flag in NoviceCard surfaces uncertainty; UX shows caveat |
| Cost blowup at chat scale | Haiku 4.5 ~$0.02/conversation at typical 5-turn length; rate-limit free tier to 3 turns/day; paid tiers 100/day |
| Regulatory: "personalized investment advice" | NoviceCard surfaces are clearly labeled "trade ideas, not advice"; ToS update; no broker integration in MVP |

---

## 10. Open questions (PR-2+)

1. Should the analog-replay simulator use `regime_narratives` as the matching key or the lower-level inputs vector? (Lean: inputs vector via cosine similarity.)
2. Voice/audio surface — does it live in `zerogex-web` (browser TTS) or as a separate streaming service?
3. Do we surface `NoviceCard` only for retail tiers, or also as an extra view for pro tiers? (Lean: also for pros as an audit summary.)
4. Daily Weather Report — separate workstream; depends on these primitives.
5. Should the Copilot be allowed to *propose* a card composition (custom strike/expiry) for STAND_DOWN cases, or only relay existing ones? (Lean: only relay — proposing custom cards reopens the hallucination surface.)
