"""Pytest session bootstrap: pin operator-tunable config to code defaults.

This must run before any test module imports ``src`` (pytest loads
conftest.py before collecting tests, so the module-level assignments
below execute first — in particular before ``src.config`` reads its
module-level ``SIGNALS_*`` constants).

Why this exists: the Makefile does ``-include .env`` then ``export``,
which pushes every key in an operator's ``.env`` into the environment of
``pytest``; ``src/config.py`` additionally calls ``load_dotenv()``. On a
configured machine that lets live tuning (feature flags, thresholds,
multipliers) override the code defaults that unit tests assert against,
so ``make test`` fails on the server while CI (which has no ``.env``)
stays green. Forcing these keys to the code defaults makes the unit
suite hermetic and identical to CI regardless of the local ``.env``.

Each value mirrors the in-code default at the cited source location;
keep them in sync if the defaults change. Tests that exercise non-default
behavior set these per-test (via monkeypatch or instance attributes), so
pinning the process-wide default here does not constrain them.
"""

import os
import tempfile

# Isolate the bulletin-tweet artifact + "latest" review-record store to a
# throwaway dir so `make test` on a configured server (where .env sets
# BULLETIN_TWEET_ARTIFACT_DIR=/var/lib/zerogex-oa/bulletin-tweets and the
# Makefile exports it) never reads or writes the REAL store — which would
# both pollute production files and leak real records into "record absent"
# assertions. Tests that exercise the artifact-dir fallback ladder delenv
# this per-test.
os.environ["BULLETIN_TWEET_ARTIFACT_DIR"] = tempfile.mkdtemp(prefix="zerogex-test-bulletin-")

# key -> (default, source of the in-code default)
_PINNED_DEFAULTS = {
    # src/api/database.py: DatabaseManager.__init__
    "FLOW_SERIES_USE_SNAPSHOT": "false",
    # src/api/freshness.py cadence profiles. The v2 freshness envelope
    # advertises the cadence the engines actually run on, so it reads these
    # live rather than copying their defaults — which means an operator .env
    # that retunes a poll interval changes what the envelope reports, and the
    # cadence assertions in test_api_v2_freshness_envelope.py would fail on
    # the server while CI (no .env) stayed green. Pin to the code defaults.
    "ANALYTICS_INTERVAL": "60",  # src/config.py
    "ANALYTICS_OFF_HOURS_INTERVAL_SECONDS": "300",  # src/analytics/main_engine.py
    "MARKET_HOURS_POLL_INTERVAL": "5",  # src/config.py
    "EXTENDED_HOURS_POLL_INTERVAL": "30",  # src/config.py
    # src/analytics/main_engine.py: AnalyticsEngine.__init__. These two flags
    # each add a DB round-trip to _get_snapshot (the cache query / the
    # extended-hours spot re-anchor fetchone). The snapshot tests mock an exact
    # fetchone/fetchall sequence for the legacy path, so an operator .env that
    # enables either flag desyncs the mocks (StopIteration) — failing on the
    # server while CI (no .env) stays green. Pin both to their code defaults;
    # cache-path tests opt in via engine.use_latest_cache on the instance.
    "ANALYTICS_USE_LATEST_CACHE": "false",
    "ANALYTICS_SPOT_ANCHORED_EXTENDED_HOURS": "false",
    # src/config.py default (also .env.example). The in-session close-stamp
    # re-anchor in _get_snapshot keys both its staleness threshold and its
    # forward-bar search window (2 x bucket) off this. Its regression test
    # places the fresh bar at open+60s, so an operator .env overriding the
    # bucket moves that bar outside the search window and the re-anchor test
    # fails on the server while CI (no .env) stays green. Pin to the default.
    "AGGREGATION_BUCKET_SECONDS": "60",
    # src/config.py module-level constants
    "MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS": "SPY,SPX,QQQ",
    "SIGNALS_BREAKOUT_SIZE_MULTIPLIER": "1.50",
    "SIGNALS_TRIGGER_THRESHOLD": "0.50",
    # src/jobs/cnbc_news.py: the bulletin runner scrapes CNBC RSS via
    # _fetch_headlines_safe. That is a live network call (slow, and the egress
    # proxy 403s it) — disable it in the unit suite so headline resolution
    # returns [] instantly. Tests that exercise the parser call it directly.
    "BULLETIN_TWEET_NEWS_ENABLED": "0",
}

for _key, _default in _PINNED_DEFAULTS.items():
    os.environ[_key] = _default

# External API credentials must never be live during the unit suite. Several
# code paths (e.g. src.jobs.bulletin_tweet's LLM narrative) make a real network
# call when their key is present — on a configured server that turns unit tests
# into slow, non-deterministic, cost-incurring live calls (an ~3 min bulletin
# run that flip-flops on whichever prose the model returns), while CI (no .env)
# stays green. Blank the keys so those paths take their offline fallback. Tests
# that deliberately exercise the credentialed path set the var per-test via
# monkeypatch (which overrides this) and stub the network client.
# RESEND_API_KEY joins the list: bulletin_tweet's built-in "X-Post Ready"
# email POSTs to the Resend API when it's set. Blank it so the runner tests
# that reach the stage/post path take the "not configured" skip instead of a
# live send; the two email tests set it per-test via monkeypatch + stub urlopen.
_BLANKED_CREDENTIALS = ("ANTHROPIC_API_KEY", "RESEND_API_KEY")
for _cred in _BLANKED_CREDENTIALS:
    os.environ[_cred] = ""
