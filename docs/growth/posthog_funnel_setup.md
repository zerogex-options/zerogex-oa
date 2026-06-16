# PostHog Funnel — Setup, the Two Headline Metrics, and How to Read the First Data

_Last updated: 2026-06-16_

The conversion funnel is wired in `zerogex-web` (`frontend/core/telemetry/*`,
`TelemetryProvider`, Stripe webhook). Events fire client-side from the browser
and server-side from the Stripe webhook, stitched to a single person via the
shared app user id passed to `identify()` / `captureServer()`.

This doc is the build sheet for the PostHog UI views, the two metrics the
business cannot run without (**trial→paid conversion** and **monthly churn**),
the cohort definitions used to drive outreach (Stream 1, Move #1 in the
roadmap), and the rubric for interpreting the first ~2 weeks of data.

Everything here is "no-code" — point-and-click in PostHog. Nothing in the
codebase needs to change to build any view below.

---

## 0. Pre-flight (one-time)

1. **PostHog project exists** and `NEXT_PUBLIC_POSTHOG_KEY` is set in the web
   deploy. Set `POSTHOG_KEY` for the server side too (or rely on the
   `NEXT_PUBLIC_POSTHOG_KEY` fallback in `posthog-server.ts`).
2. **Confirm events arriving.** PostHog → Activity → Live events. Reload
   `zerogex.io` and watch a `$pageview` come in. Sign in and watch the
   `$identify` follow. If neither shows up, the key is wrong or the deploy
   hasn't picked it up — fix this first, nothing below works otherwise.
3. **Confirm the six funnel events appear in the event taxonomy** (Data
   management → Events). Some only fire on conditions, so on day one expect
   to see `$pageview`, `$identify`, `signup`, and `first_value`. The Stripe
   events (`trial_started`, `subscription_paid`, `subscription_cancelled`,
   plus `checkout_started`) appear the moment the first new subscriber
   transitions through them.

The canonical event list (single source of truth in the codebase) is in
`zerogex-web/frontend/core/telemetry/events.ts`.

---

## 1. The headline funnel: `Conversion funnel`

PostHog → **Product analytics** → **+ New insight** → **Funnel**.

**Steps (in this order — order is what makes it a funnel):**

| # | Event                   | Notes                                                    |
|---|-------------------------|----------------------------------------------------------|
| 1 | `signup`                | Client. Fires once per new account from `/register`.     |
| 2 | `first_value`           | Client. First successful Gamma Exposure render.          |
| 3 | `checkout_started`      | Client. Click on the Subscribe button on `/pricing`.     |
| 4 | `trial_started`         | Server. Stripe webhook: status enters a trial.           |
| 5 | `subscription_paid`     | Server. Stripe webhook: status becomes `active` (incl. trial→paid). |

**Settings:**
- Conversion window: **14 days**. (7-day Stripe trial + a couple of days of
  consideration; longer windows hide whether a step is actually the leak.)
- Funnel type: **sequential** (default).
- Breakdown — leave empty for the top-line; add a copy of this insight
  broken down by `utm_source`, `utm_campaign`, and `$initial_referring_domain`
  to compare X traffic vs ChatGPT vs direct.

**Save as:** `Conversion Funnel — Site to Paid`.
**Pin** to a dashboard called `Growth`.

### What "good" looks like at our stage (cold-start baseline)

| Step transition                       | Healthy             | Watch                 | Leak                    |
|---------------------------------------|---------------------|-----------------------|-------------------------|
| signup → first_value                  | 70%+                | 50–70%                | <50% (onboarding broken)|
| first_value → checkout_started        | 25%+                | 15–25%                | <15% (pricing/value mismatch) |
| checkout_started → trial_started      | 80%+                | 60–80%                | <60% (Stripe friction)  |
| trial_started → subscription_paid     | 35%+                | 20–35%                | <20% (trial experience fails) |
| signup → subscription_paid (end-to-end)| 8%+                | 4–8%                  | <4%                     |

Today's measured end-to-end is **~5.4%** (15/277). That puts us squarely in
"watch" — not catastrophic, but not yet converting at the rate that justifies
heavy paid acquisition. The funnel above tells us which single step is
costing us the most.

---

## 2. Headline metric #1 — **Trial → Paid conversion**

PostHog → **+ New insight** → **Funnel**.

| # | Event                | Filter            |
|---|----------------------|-------------------|
| 1 | `trial_started`      | _none_            |
| 2 | `subscription_paid`  | _none_            |

**Settings:**
- Conversion window: **10 days** (covers a 7-day trial + a couple of grace
  days for late billing).
- Trend: switch to the **Trends** view of this funnel and look at the
  rolling 30-day conversion rate. This is the single number the roadmap
  calls out as governing every ARR projection.

**Save as:** `Metric — Trial→Paid (rolling 30d)`. Pin to `Growth`.

### Reading it
- **>35%** — the product is converting the people who try it; pour fuel on
  acquisition.
- **20–35%** — the trial works but isn't selling. Inspect the trial
  experience (Move #1 outreach), and consider trial length tweaks.
- **<20%** — the trial is teaching the wrong lesson. Re-examine onboarding,
  the "aha" path, and what the trial unlocks vs the free dashboards.

---

## 3. Headline metric #2 — **Monthly churn**

PostHog → **+ New insight** → **Trends**.

**Series A — cancellations:**
- Event: `subscription_cancelled`.
- Math: **Unique users** (so re-cancelling the same Stripe sub doesn't
  double-count).
- Aggregation: **Total** per month.

**Series B — active subscriber base (denominator):**
- The roadmap explicitly does not yet wire a daily `active_subscribers`
  event from the backend. Until that lands, the denominator comes from the
  Stripe Dashboard → Customers → Subscribers count at month-end. Compute
  monthly churn as:

      churn_% = unique_subscription_cancelled_in_month / subscribers_at_month_start

  For now, do this calculation in the dashboard's notes section — keep a
  rolling text block with the month, count, denominator, and the resulting
  %. Two months of data is enough to see the trend.

**Save as:** `Metric — Subscription cancellations / month`. Pin to `Growth`.

### Reading it
Trading-tool category baseline is 5–10% monthly. Anything sustained above
**10%** is a structural problem; the roadmap names this as the #1 risk that
caps every ARR projection. The Discord move (Stream 1 #2) is the primary
durability lever — measure churn _before_ and _after_ Discord lands.

---

## 4. Cohorts used to drive outreach (Move #1)

The non-converter outreach in `non_converter_outreach.md` uses three cohorts.
Build each in PostHog → **Data management** → **Cohorts** → **+ New cohort**.

### Cohort A — "Signed up, never started a trial"
- Match users who: performed `signup` in the **last 30 days**.
- AND who have **not** performed `trial_started` in the **last 30 days**.
- AND who have **not** performed `subscription_paid` in the **last 30 days**.
- Static: leave unchecked (we want it to update as more users qualify).

**Save as:** `Outreach — Signed up, no trial`.

### Cohort B — "Started a trial, didn't convert"
- Match users who: performed `trial_started` **more than 8 days ago** (the
  trial has ended).
- AND who have **not** performed `subscription_paid` in the **last 30 days**.

**Save as:** `Outreach — Trial drop-off`.

### Cohort C — "Founding members on trial, deciding by July 1"
- Match users who: performed `trial_started` AND identify property
  `founding_eligible = true` (added to `identify()` on every session — see
  `ClientLayout.tsx`).
- AND **not** `subscription_paid`.
- AND first seen **before 2026-06-01**.

**Save as:** `Outreach — Founding cohort still deciding`.

### Exporting for outreach
For each cohort, **Persons** tab → **Export CSV**. Email + app user id come
out of the box; that's enough to cross-reference the website's user table
and send a personalized email. (Don't email out of PostHog directly — keep
that workflow in Resend or whatever the production transactional sender is.)

---

## 5. Source attribution view — _is it X, ChatGPT, or direct?_

PostHog → **+ New insight** → **Trends**.

- Event: `$pageview` filtered to `$pathname = /`.
- Breakdown: `$initial_referring_domain`.
- Math: **Unique users**, last 30 days.

**Save as:** `Acquisition — Landing visits by source`. Pin to `Growth`.

Then build the same view but with **Series math = Funnel conversion to
`signup`** (PostHog supports this directly from a Trends insight by adding
a goal). That gives the **per-source signup rate**, which is the
single most informative slice for steering acquisition effort. The
already-visible `utm_source=chatgpt.com` traffic showed up at noticeable
volume in early week 1 — quantify whether it converts at a rate worth
optimizing for, or whether X traffic does better despite lower volume.

---

## 6. The first-data interpretation rubric

The point of this funnel at 277 signups is **not** statistical certainty —
the sample's too small to make confident A/B decisions. The point is to
**spot the obvious leak** so you know which step deserves a manual fix this
week. Use this rubric in order:

1. **Find the worst step in the headline funnel** (largest absolute drop).
2. **Open the "Funnels — User paths" tab on that step.** PostHog will let
   you list every user who dropped at that step. Sort by `signup` date
   descending so the freshest drop-offs come first.
3. **Cross-reference each drop-off against the cohort CSVs** from §4 — the
   handful that overlap are your highest-priority outreach (most recent,
   tried the product, didn't convert).
4. **For each user** in the priority list, open their PostHog Person view
   and read the event timeline. The two questions you want answered:
     - Did they actually see the dashboard? (Was `first_value` fired? On
       which symbol? How long after `signup`?)
     - Did they get to pricing? (Was `$pageview /pricing` fired?
       Was `checkout_started` fired?)
   The pattern of presence/absence usually reads like a short story.
5. **Send the outreach template** from `non_converter_outreach.md` that
   matches their story. Personalize one line per recipient — that's what
   makes them reply.

### What to do with the answers
- If 50%+ of replies are some flavor of **"I didn't realize what was
  paywalled"** → the leak is at `first_value → checkout_started` and the
  fix is on the pricing page / paywalled-feature messaging.
- If 50%+ are **"the trial wasn't long enough to evaluate"** or **"I lost
  the email reminder"** → the leak is at `trial_started → subscription_paid`
  and the fix is the trial-end email cadence (and possibly a longer trial
  for the next cohort).
- If 50%+ are **"I'm waiting for X feature"** → that's product, not
  funnel. Tag the request in a single doc; build the most-asked thing
  next. **Don't change the funnel** in this case — the data says people
  understood the product fine, they just want one more thing.

Treat 12–15 reply conversations as enough signal to act. We don't need
significance; we need direction.

---

## 7. Things deliberately NOT built yet

Documented to avoid forgetting why they're missing:

- **Server-side daily `active_subscribers` snapshot event.** Would replace
  the Stripe-Dashboard-denominator workaround in §3. Worth one hour of
  backend work the moment monthly churn becomes a tracked OKR.
- **A/B experiments on pricing copy.** Sample size is far too small;
  significance won't be reached for months. Re-evaluate at 1,500+
  signups/mo.
- **Session replay.** Deliberately OFF in `posthog-client.ts` for a
  financial app — re-enable only after a privacy review and a
  user-visible disclosure.
- **Server-side `feature_used` events for each premium dashboard.** Useful
  for understanding which features drive retention; defer until we have
  a 100+ active-subscriber base where the per-feature segments are large
  enough to read.

When any of these unblocks a decision the funnel above can't answer, build
it. Not before.
