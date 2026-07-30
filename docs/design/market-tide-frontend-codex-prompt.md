# Codex Prompt: Market Tide Frontend Page

The text between **PROMPT START** and **PROMPT END** is intended to be copied
verbatim into Codex while it is opened in the `zerogex-options/zerogex-web`
repository.

---

## PROMPT START

You are working in the `zerogex-options/zerogex-web` frontend repository.

Implement a polished frontend MVP for the new **Market Tide** metric. This must
be a **completely new page**, not a dashboard card, modal, or addition to an
existing metrics page. Add a **Market Tide** navigation item beneath the
existing **Metrics** section of the navigation menu.

### Before editing

1. Find and read every applicable `AGENTS.md` file.
2. Inspect and follow the repository's established conventions for routing,
   navigation, page layouts, API clients, authentication, query caching,
   polling, components, charts, responsive design, accessibility, and tests.
3. Reuse the existing UI system, icons, query library, formatting helpers, and
   charting tools. Do not add a new library when an existing dependency can do
   the job.
4. Do not modify the backend or invent response fields.
5. Do not add Market Tide to the main dashboard. It belongs only on its own
   page under **Metrics**.
6. Commit all changes on the current branch and create a pull request.

### Route and navigation

- Add a dedicated page using `/market-tide`, unless the existing Metrics
  routes use a required nested convention such as `/metrics/market-tide`. In
  that case, follow the existing convention.
- Add **Market Tide** beneath the existing **Metrics** navigation section.
- Use an existing icon suggesting breadth, waves, market direction, or
  pressure.
- Ensure active-route highlighting, collapsed navigation, mobile navigation,
  and permissions behave like adjacent Metrics entries.

### Backend endpoint

Request:

```http
GET /api/flow/market-tide?window=15
```

Supported `window` values are `5`, `15`, `30`, and `60`. Default to `15`.
Use the existing authenticated API client; this endpoint uses the existing
options-flow scope.

Add TypeScript types matching this exact contract:

```ts
export type MarketTideLabel =
  | "strong_bullish"
  | "bullish"
  | "neutral"
  | "bearish"
  | "strong_bearish"
  | "insufficient_data";

export type MarketTideGammaLabel =
  | "amplifying"
  | "neutral"
  | "dampening";

export interface MarketTideComponent {
  symbol: string;
  flow_score: number;
  gamma_score: number;
  amplifier: number;
  weight: number;
  contribution: number;
}

export interface MarketTideResponse {
  timestamp: string;
  score: number | null;
  label: MarketTideLabel;
  flow_direction: number;
  gamma_regime: number;
  gamma_label: MarketTideGammaLabel;
  bullish_breadth_pct: number;
  bearish_breadth_pct: number;
  neutral_breadth_pct: number;
  participation_pct: number;
  eligible_symbols: number;
  configured_symbols: number;
  stale_symbols: string[];
  leaders: MarketTideComponent[];
  laggards: MarketTideComponent[];
}
```

Important semantics:

- `score` normally ranges from `-100` to `+100`.
- Positive is bullish; negative is bearish.
- `score` is `null` when fewer than 60% of configured symbols have fresh
  gamma and flow data.
- The backend `label` is authoritative. Do not invent frontend thresholds.
- `flow_direction` supplies direction.
- Negative `gamma_regime` indicates amplification; positive indicates
  dampening. Gamma is not independently bullish or bearish.
- Breadth and participation values are percentages from 0 to 100.
- Contributor `weight` is a fraction and should be formatted as a percentage.
- Contributor `contribution` is signed and is measured before the final score's
  `×100` display scaling.
- Contributor and stale-symbol arrays can be empty.
- Handle unknown future labels defensively without crashing.

### Page header

Build a standard Metrics page header containing:

- Title: **Market Tide**
- Description: **Market-wide options pressure adjusted for the dealer gamma
  regime.**
- Last-updated timestamp using the existing application formatter.
- A segmented window selector: **5m**, **15m**, **30m**, and **60m**.
- Default selection: **15m**.

Changing the window must refetch the endpoint with that window. If adjacent
Metrics pages synchronize filters to the URL, do the same here.

### Primary Market Tide gauge

Create an accessible horizontal diverging gauge:

- Range: `-100` to `+100`.
- Center marker at `0`.
- Bearish on the left and bullish on the right.
- Show the numeric score and formatted backend label prominently.
- Format labels as Strong Bearish, Bearish, Neutral, Bullish, Strong Bullish,
  and Insufficient Data.
- Do not communicate direction through red/green color alone.
- Clamp only the visual marker position to `[-100, 100]`; guard against
  non-finite values so CSS never receives `NaN` or `Infinity`.
- Use the backend label rather than deriving a label from the numeric score.

### Insufficient-data state

When `score === null` or `label === "insufficient_data"`:

- Do not render a marker at zero or call the state Neutral.
- Display **Insufficient Data** in the primary gauge area.
- Explain: **The Market Tide score is withheld until at least 60% of supported
  symbols have fresh gamma and flow data.**
- Continue to display participation, eligible/configured counts, and stale
  symbols.
- Keep the window selector operational.
- Treat this as a successful API response, not an error.

### Supporting metric cards

Add cards for:

1. **Flow Direction**
   - Show `flow_direction` to two decimals.
   - Positive: Call-led / bullish.
   - Negative: Put-led / bearish.
   - Near zero: Balanced.
   - This descriptor must not replace the authoritative Market Tide label.

2. **Gamma Regime**
   - Show `gamma_regime` to two decimals and the backend `gamma_label`.
   - Amplifying explanation: Negative gamma can strengthen moves in the
     direction of options pressure.
   - Dampening explanation: Positive gamma can absorb or moderate directional
     options pressure.
   - Neutral explanation: Dealer gamma is not materially amplifying or
     dampening options pressure.

3. **Participation**
   - Show `eligible_symbols` of `configured_symbols`.
   - Show `participation_pct` and a progress indicator.
   - Visually warn below 60%, without treating it as an API failure.

### Breadth

Create a labeled stacked breadth bar showing:

- Bullish breadth
- Neutral breadth
- Bearish breadth

Show the exact returned percentage values in text. Normalize only visual
segment widths defensively if floating-point rounding means the values do not
sum to exactly 100.

### Leaders and laggards

Add an explainability section with separate **Leaders** and **Laggards** views.
For every component show:

- Symbol
- Signed contribution
- Flow score
- Gamma score
- Amplifier
- Weight as a percentage

Use semantic tables on desktop if consistent with the project and responsive
cards or the established responsive-table pattern on mobile. Add concise,
keyboard-accessible tooltips defining each metric. If symbols elsewhere link
to ticker detail pages, use the same link pattern.

Empty states:

- **No positive contributors in this window.**
- **No negative contributors in this window.**

### Stale symbols

When stale symbols exist, show a visually secondary disclosure labeled:

**Stale or unavailable symbols (N)**

Render symbols as compact badges or text. Increase its warning emphasis only
when participation is below 60%.

### Fetching behavior

- Use the existing query/data-fetching abstraction.
- Include `window` in the query key.
- Poll approximately every 30 seconds, consistent with the backend cache,
  unless the app has a standard market-data refresh interval.
- Use query-library polling, not an overlapping manual timer.
- Preserve the prior successful value during window changes/background
  refreshes when supported by existing conventions.
- Surface background refreshing subtly rather than replacing populated content
  with a full loading screen.
- Respect existing abort signals, visibility handling, authentication, and
  retry conventions.
- Do not manufacture a historical chart from browser polling or local storage;
  the backend currently provides only a current snapshot.

### Loading, error, and defensive states

Implement:

- Initial skeleton loading state.
- Network/API error state with retry.
- Valid insufficient-participation state.
- Empty leaders, laggards, and stale lists.
- Safe unknown-label presentation.
- Missing/invalid timestamp fallback.
- Protection from malformed or non-finite numeric values.

Never display `NaN`, `Infinity`, or a fabricated zero score.

### Accessibility and responsiveness

- Give the gauge an accessible name, value, and textual state.
- Ensure the window selector is keyboard accessible.
- Use semantic table markup where applicable.
- Make tooltips accessible by keyboard.
- Do not depend on color alone.
- Respect reduced-motion preferences.
- Match existing contrast requirements.
- Make the complete page usable at desktop and mobile widths.

### Tests

Use the repository's existing testing and API-mocking stack. At minimum test:

1. The new route renders as a standalone page.
2. Market Tide appears under the Metrics navigation section.
3. A populated bullish response.
4. A populated bearish response.
5. `score: null` and `insufficient_data` do not display zero/Neutral.
6. Selecting 5m, 15m, 30m, and 60m requests the correct window.
7. Leaders and laggards render and format values correctly.
8. Empty contributor states.
9. Stale-symbol disclosure.
10. Initial loading state.
11. API error and retry.
12. Unknown labels do not crash.
13. Malformed/non-finite values cannot create broken output.
14. Gauge positioning at `-100`, `0`, and `+100`.
15. Mobile layout behavior if the repository has viewport/component tests.

Use this populated fixture:

```json
{
  "timestamp": "2026-07-29T15:35:00Z",
  "score": 61.4,
  "label": "strong_bullish",
  "flow_direction": 0.48,
  "gamma_regime": -0.27,
  "gamma_label": "amplifying",
  "bullish_breadth_pct": 68.2,
  "bearish_breadth_pct": 18.2,
  "neutral_breadth_pct": 13.6,
  "participation_pct": 91.7,
  "eligible_symbols": 22,
  "configured_symbols": 24,
  "stale_symbols": ["XYZ", "ABC"],
  "leaders": [
    {
      "symbol": "SPY",
      "flow_score": 0.72,
      "gamma_score": -0.31,
      "amplifier": 1.155,
      "weight": 0.15,
      "contribution": 0.12474
    },
    {
      "symbol": "QQQ",
      "flow_score": 0.58,
      "gamma_score": -0.18,
      "amplifier": 1.09,
      "weight": 0.14,
      "contribution": 0.088508
    }
  ],
  "laggards": [
    {
      "symbol": "IWM",
      "flow_score": -0.26,
      "gamma_score": 0.12,
      "amplifier": 0.94,
      "weight": 0.09,
      "contribution": -0.021996
    }
  ]
}
```

Use this insufficient-data fixture:

```json
{
  "timestamp": "2026-07-29T15:35:00Z",
  "score": null,
  "label": "insufficient_data",
  "flow_direction": 0.11,
  "gamma_regime": -0.08,
  "gamma_label": "neutral",
  "bullish_breadth_pct": 50.0,
  "bearish_breadth_pct": 25.0,
  "neutral_breadth_pct": 25.0,
  "participation_pct": 50.0,
  "eligible_symbols": 12,
  "configured_symbols": 24,
  "stale_symbols": ["AAPL", "AMD", "AMZN", "GOOGL", "META", "MSFT", "NVDA", "SPX", "TSLA", "XYZ", "ABC", "DEF"],
  "leaders": [],
  "laggards": []
}
```

### Visual verification

Run the app and inspect both populated and insufficient-data states. Take
screenshots at approximately:

- Desktop: `1440 × 1000`
- Mobile: `390 × 844`

This is a perceptible web change, so screenshots are required. Verify that
Market Tide is a standalone page and that its nav entry appears specifically
under Metrics.

### Required checks and final response

Run the repository's formatter, linter, type checker, unit/component tests,
production build, and relevant E2E tests. In the final response:

- Summarize significant changes with file citations.
- List exact commands and pass/fail/warning status.
- Attach or identify screenshot paths.
- Report environmental limitations precisely.
- Report the commit hash and pull-request title.

## PROMPT END
