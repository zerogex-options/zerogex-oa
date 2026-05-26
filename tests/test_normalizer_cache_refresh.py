"""Tests for the component_normalizer_cache refresh tool."""

from __future__ import annotations

import math
from datetime import datetime

import pytest
import pytz

from src.tools.normalizer_cache_refresh import (
    DEFAULT_DEPLOY_CUTOFF,
    DEPLOY_CUTOFF_ENV,
    FIELD_SPECS,
    MIN_SAMPLES,
    Distribution,
    _fetch_samples,
    _resolve_deploy_cutoff,
    _summarize,
    refresh,
)

# A fixed post-rescale cutoff used across the binding tests.  2026-05-16
# 00:00 US/Eastern is 04:00Z (EDT, UTC-4).
_CUTOFF = pytz.timezone("US/Eastern").localize(datetime(2026, 5, 16, 0, 0))


def test_summarize_returns_none_below_min_samples():
    assert _summarize([1.0, -2.0, 3.0]) is None
    assert _summarize([float(i) for i in range(MIN_SAMPLES - 1)]) is None


def test_summarize_uses_abs_for_percentiles_signed_for_std():
    # 100 evenly-spaced signed samples -50..49.  |x| percentiles look only
    # at magnitude; std should reflect the signed dispersion.
    samples = [float(i) for i in range(-50, 50)]
    dist = _summarize(samples)
    assert dist is not None
    # 95th percentile of |x| over [-50..49] is around 47-48.
    assert 45 <= dist.p95 <= 50
    # Median magnitude of evenly-spaced symmetric data is ~25.
    assert 20 <= dist.p50 <= 30
    # Signed std ≈ 29 for uniform [-50, 49].
    assert 25 <= dist.std <= 35
    assert dist.sample_size == 100


def test_summarize_skips_nan_and_none():
    samples = [1.0, 2.0, float("nan"), None, 3.0] + [float(i) for i in range(MIN_SAMPLES)]
    dist = _summarize(samples)
    assert dist is not None
    # NaN + None dropped; finite count = 3 + MIN_SAMPLES.
    assert dist.sample_size == 3 + MIN_SAMPLES


def test_summarize_zero_only_yields_zero_distribution():
    dist = _summarize([0.0] * MIN_SAMPLES)
    assert dist is not None
    assert dist.p05 == 0.0
    assert dist.p50 == 0.0
    assert dist.p95 == 0.0
    assert dist.std == 0.0


def test_field_specs_cover_all_normalizer_consumers():
    """Every name read from ``ctx.extra['normalizers']`` in the signal code
    should have a matching FieldSpec — otherwise the populator silently
    leaves that field unscaled.

    Pinned to the exact set: an accidental rename or unintentional
    addition is a test failure (the populator's writes are not free, so
    every FieldSpec must have an active consumer).  The historical
    ``smart_money_volume_delta`` / ``smart_money_premium`` specs were
    removed when flow_smart_money was decommissioned — those rows no
    longer have a reader."""
    signal_consumers = {
        "dealer_vanna_exposure",
        "dealer_charm_exposure",
        "local_gex",
        "net_gex_delta",
        "call_flow_delta",
        "put_flow_delta",
    }
    actual = {spec.name for spec in FIELD_SPECS}
    assert actual == signal_consumers, (
        f"unexpected FieldSpec set: "
        f"extra={actual - signal_consumers}, "
        f"missing={signal_consumers - actual}"
    )


def test_field_specs_placeholder_count_matches_binding_contract():
    """A plain spec binds (symbol, window_days) -> 2 placeholders; a
    deploy-cutoff-filtered spec also binds the cutoff -> 3, and that 3rd
    placeholder must be the trailing ``timestamp >= %s`` floor."""
    for spec in FIELD_SPECS:
        expected = 3 if spec.deploy_cutoff_filtered else 2
        assert spec.query.count("%s") == expected, (
            f"{spec.name}: expected {expected} %s placeholders, " f"got {spec.query.count('%s')}"
        )
        if spec.deploy_cutoff_filtered:
            assert "AND timestamp >= %s" in " ".join(spec.query.split()), (
                f"{spec.name}: cutoff-filtered spec must carry an explicit "
                "'AND timestamp >= %s' floor"
            )


def test_only_dealer_vanna_charm_are_cutoff_filtered():
    """The 2026-05-15 rescale touched dealer vanna/charm only — no other
    sampled field may be silently date-floored."""
    filtered = {s.name for s in FIELD_SPECS if s.deploy_cutoff_filtered}
    assert filtered == {"dealer_vanna_exposure", "dealer_charm_exposure"}


def test_distribution_is_immutable():
    dist = Distribution(p05=1.0, p50=2.0, p95=3.0, std=0.5, sample_size=10)
    with pytest.raises(Exception):
        dist.p95 = 99.0  # frozen dataclass


def test_summarize_reflects_realistic_spy_magnitudes():
    """SPY-scale dealer_vanna_exposure samples produce p95 in the
    hundreds-of-millions to billions range — the calibration goal that
    the static _VC_NORM default failed to hit."""
    rng_samples = [
        # Mix of positive/negative around 0 with magnitudes ~$200M-$1B.
        ((-1) ** i) * (2.0e8 + (i % 7) * 1.0e8)
        for i in range(200)
    ]
    dist = _summarize(rng_samples)
    assert dist is not None
    assert dist.p95 >= 5e8, f"p95={dist.p95:.2e} too small for SPY-scale samples"
    assert math.isfinite(dist.std) and dist.std > 0


# --- deploy-cutoff resolution -------------------------------------------------

_UTC = pytz.UTC


def test_default_cutoff_is_post_rescale_et_midnight():
    """Default == 2026-05-16 00:00 US/Eastern == 2026-05-16 04:00Z (EDT).
    The 2026-05-15 ~22:40 ET deploy is strictly before this instant."""
    assert DEFAULT_DEPLOY_CUTOFF == "2026-05-16"
    cut = _resolve_deploy_cutoff(None)
    assert cut.tzinfo is not None
    assert cut.astimezone(_UTC) == datetime(2026, 5, 16, 4, 0, tzinfo=_UTC)


@pytest.mark.parametrize("blank", ["", "   ", "\t\n"])
def test_blank_cutoff_falls_back_to_default(blank):
    assert _resolve_deploy_cutoff(blank) == _resolve_deploy_cutoff(None)


def test_date_only_cutoff_is_et_midnight_dst_correct():
    cut = _resolve_deploy_cutoff("2026-06-01")
    assert (cut.year, cut.month, cut.day, cut.hour, cut.minute) == (2026, 6, 1, 0, 0)
    # June -> EDT (UTC-4).
    assert cut.utcoffset().total_seconds() == -4 * 3600


def test_naive_datetime_cutoff_interpreted_as_eastern():
    cut = _resolve_deploy_cutoff("2026-05-16 09:30")
    assert (cut.hour, cut.minute) == (9, 30)
    assert cut.utcoffset().total_seconds() == -4 * 3600
    assert cut.astimezone(_UTC) == datetime(2026, 5, 16, 13, 30, tzinfo=_UTC)


def test_offset_aware_cutoff_is_respected_not_relocalized():
    """A value that already carries an offset must NOT be reinterpreted in
    ET (that would shift the instant by 4-5h)."""
    cut = _resolve_deploy_cutoff("2026-05-16T00:00:00+00:00")
    assert cut.utcoffset().total_seconds() == 0
    assert cut.astimezone(_UTC) == datetime(2026, 5, 16, 0, 0, tzinfo=_UTC)


@pytest.mark.parametrize("bad", ["garbage", "2026-13-40", "not-a-date", "2026/05/16"])
def test_malformed_cutoff_fails_closed(bad):
    """Fail loud: a typo must never silently degrade to 'no cutoff' and
    let the refresh sample pre-rescale vanna again."""
    with pytest.raises(ValueError):
        _resolve_deploy_cutoff(bad)


# --- fake-cursor harness (repo record-the-SQL style) --------------------------

_SPEC_BY_NORM_SQL = {" ".join(s.query.split()): s.name for s in FIELD_SPECS}


def _norm(sql: str) -> str:
    return " ".join(sql.split())


class _ScriptedCursor:
    """Records (sql, params); fetchall() replays the rows scripted for the
    sample query last executed (keyed by its exact FieldSpec SQL).  An
    INSERT (the upsert) maps to no field, so fetchall() yields nothing."""

    def __init__(self, samples_by_field=None):
        self._samples = samples_by_field or {}
        self.executed: list = []
        self._last_field = None

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self._last_field = _SPEC_BY_NORM_SQL.get(_norm(sql))

    def fetchall(self):
        return [(v,) for v in self._samples.get(self._last_field, [])]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _ScriptedConn:
    def __init__(self, cur):
        self._cur = cur

    def cursor(self):
        return self._cur

    def commit(self):
        pass

    def rollback(self):
        pass


def _spec(name):
    return next(s for s in FIELD_SPECS if s.name == name)


def _selects_for(cur, field):
    norm_sql = _norm(_spec(field).query)
    return [(sql, p) for sql, p in cur.executed if _norm(sql) == norm_sql]


def _cache_inserts(cur):
    return [p for sql, p in cur.executed if "INSERT INTO component_normalizer_cache" in _norm(sql)]


# --- _fetch_samples binding ---------------------------------------------------


def test_fetch_samples_binds_cutoff_only_for_filtered_specs():
    cur = _ScriptedCursor()
    _fetch_samples(cur, _spec("dealer_vanna_exposure"), "SPY", 20, _CUTOFF)
    sql, params = cur.executed[-1]
    assert "AND timestamp >= %s" in _norm(sql)
    assert params == ("SPY", "20", _CUTOFF)


def test_fetch_samples_plain_spec_binds_two_params_no_cutoff():
    cur = _ScriptedCursor()
    _fetch_samples(cur, _spec("local_gex"), "SPY", 20, _CUTOFF)
    sql, params = cur.executed[-1]
    assert params == ("SPY", "20")
    assert _CUTOFF not in params
    # Plain specs have only the rolling-window floor, not the deploy floor.
    assert "AND timestamp >= %s" not in _norm(sql)


# --- refresh() end-to-end -----------------------------------------------------


def test_refresh_floors_vanna_charm_and_skips_below_min_post_cutoff():
    """The whole point: the cutoff is bound onto the dealer vanna/charm
    sample queries (and only those), and a post-cutoff sample below
    MIN_SAMPLES leaves the field UNSET (no upsert) so vanna_charm_flow
    keeps its corrected fallback constant instead of a low-confidence
    normalizer."""
    samples = {
        # Plenty post-cutoff -> upserts.
        "dealer_vanna_exposure": [((-1) ** i) * (1.0e6 + i) for i in range(40)],
        # Sparse post-cutoff (cutoff filtered most rows out) -> must skip.
        "dealer_charm_exposure": [1.0e8, -2.0e8, 3.0e8, -4.0e8, 5.0e8],
        # A plain (non-cutoff) field that is well-populated -> upserts.
        # Proves charm's skip is the cutoff/min rule, not a generic stall.
        "local_gex": [float(i) for i in range(60)],
    }
    cur = _ScriptedCursor(samples)
    results = refresh(_ScriptedConn(cur), ["spy"], window_days=20, deploy_cutoff=_CUTOFF)

    # Cutoff is bound (3rd param) on both dealer specs...
    for field in ("dealer_vanna_exposure", "dealer_charm_exposure"):
        sel = _selects_for(cur, field)
        assert sel, f"{field} sample query never ran"
        assert sel[0][1] == ("SPY", "20", _CUTOFF)
    # ...and NOT on a plain field.
    assert _selects_for(cur, "local_gex")[0][1] == ("SPY", "20")

    upserted = {p[1] for p in _cache_inserts(cur)}  # field_name is param idx 1
    assert "dealer_vanna_exposure" in upserted
    assert "local_gex" in upserted
    assert (
        "dealer_charm_exposure" not in upserted
    ), "below-min post-cutoff charm sample must be left unset, not written"

    assert isinstance(results["SPY"]["dealer_vanna_exposure"], Distribution)
    assert results["SPY"]["dealer_charm_exposure"] is None


def test_refresh_dry_run_never_upserts_even_when_well_populated():
    samples = {"dealer_vanna_exposure": [((-1) ** i) * (1.0e6 + i) for i in range(40)]}
    cur = _ScriptedCursor(samples)
    refresh(
        _ScriptedConn(cur),
        ["SPY"],
        window_days=20,
        dry_run=True,
        deploy_cutoff=_CUTOFF,
    )
    assert _cache_inserts(cur) == []


def test_refresh_resolves_cutoff_from_env_when_not_passed(monkeypatch):
    """deploy_cutoff=None -> read NORMALIZER_DEPLOY_CUTOFF (the documented
    env-var control), not a hardcoded date."""
    monkeypatch.setenv(DEPLOY_CUTOFF_ENV, "2026-07-04")
    samples = {"dealer_vanna_exposure": [((-1) ** i) * (1.0e6 + i) for i in range(40)]}
    cur = _ScriptedCursor(samples)
    refresh(_ScriptedConn(cur), ["SPY"], window_days=20)
    bound_cutoff = _selects_for(cur, "dealer_vanna_exposure")[0][1][2]
    assert bound_cutoff == _resolve_deploy_cutoff("2026-07-04")
