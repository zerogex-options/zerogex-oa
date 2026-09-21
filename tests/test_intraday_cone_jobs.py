"""Tests for the intraday cone writer and its grader.

The model tests cover the math.  These cover the two places a correct model
can still produce a dishonest track record: a writer that commits claims it
should not, and a grader that reads the wrong window.

The load-bearing assertions here are the ones about the grading window being
half-open — the anchor bar excluded — and about the abandoned state being
distinct from both pending and graded.  Both are ways the scoreboard could
quietly drift in its own favor without anything looking broken.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

ET = ZoneInfo("America/New_York")


def _reload(name: str):
    for mod in list(sys.modules):
        if mod.startswith(f"src.jobs.{name}") or mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    import importlib

    return importlib.import_module(f"src.jobs.{name}")


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


def _fake_writer_db(
    *,
    quote=600.0,
    gex=None,
    morning=None,
    extremes=None,
    inserted=4,
    gex_ts=None,
):
    now = datetime(2026, 9, 21, 11, 0, tzinfo=ET)
    if gex is None:
        gex = {
            "spot_price": Decimal("600.00"),
            "call_wall": Decimal("606.00"),
            "put_wall": Decimal("594.00"),
            "gamma_flip": Decimal("601.00"),
            "net_gex_at_spot": Decimal("400000000"),
            "timestamp": gex_ts if gex_ts is not None else now - timedelta(minutes=1),
        }
    db = type("FakeDB", (), {})()
    db.connect = AsyncMock(return_value=None)
    db.disconnect = AsyncMock(return_value=None)
    db.get_latest_quote = AsyncMock(
        return_value={"close": Decimal(str(quote))} if quote is not None else None
    )
    db.get_latest_gex_summary = AsyncMock(return_value=gex)
    db.get_daily_forecast = AsyncMock(
        return_value=morning if morning is not None else {"implied_move": Decimal("4.50")}
    )
    db.get_bar_extremes_between = AsyncMock(
        return_value=extremes
        if extremes is not None
        else {"window_low": Decimal("598.50"), "window_high": Decimal("602.00"), "bars": 90}
    )
    db.insert_intraday_cone = AsyncMock(return_value=inserted)
    return db


def _writer_args(mod, **overrides):
    args = mod._parse_args([])
    args.symbol = "SPY"
    args.date = "2026-09-21"
    args.at = "2026-09-21T11:00"
    args.dry_run = False
    args.allow_non_trading_day = False
    args.allow_off_window = False
    for k, v in overrides.items():
        setattr(args, k, v)
    return args


@pytest.mark.asyncio
async def test_writer_skips_a_non_trading_day(monkeypatch):
    mod = _reload("intraday_cone_writer")
    monkeypatch.setattr(mod, "DatabaseManager", lambda: pytest.fail(
        "DatabaseManager must not be constructed on a weekend"
    ))
    # 2026-09-20 is a Sunday.
    assert await mod._run(_writer_args(mod, date="2026-09-20", at="2026-09-20T11:00")) == 0


@pytest.mark.asyncio
async def test_writer_skips_outside_the_fire_window(monkeypatch, caplog):
    """No cone before 09:45 or after 15:30 — the window exists so every
    published claim has a horizon that can actually complete."""
    mod = _reload("intraday_cone_writer")
    monkeypatch.setattr(mod, "DatabaseManager", lambda: pytest.fail(
        "DatabaseManager must not be constructed outside the fire window"
    ))
    for at in ("2026-09-21T09:31", "2026-09-21T15:45", "2026-09-21T18:00"):
        assert await mod._run(_writer_args(mod, at=at)) == 0
    assert "outside the fire window" in caplog.text


@pytest.mark.asyncio
async def test_writer_commits_one_row_per_horizon(monkeypatch):
    mod = _reload("intraday_cone_writer")
    fake = _fake_writer_db()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)

    assert await mod._run(_writer_args(mod)) == 0
    fake.insert_intraday_cone.assert_awaited_once()
    rows = fake.insert_intraday_cone.await_args.args[0]
    assert [r["horizon_min"] for r in rows] == [30, 60, 90, 120]
    for r in rows:
        assert r["symbol"] == "SPY"
        assert r["session_date"] == date(2026, 9, 21)
        # target_ts is exactly the horizon past the anchor.
        assert r["target_ts"] - r["forecast_ts"] == timedelta(minutes=r["horizon_min"])
        assert r["band_low"] < float(r["anchor_spot"]) < r["band_high"]
        assert 0.0 < r["hold_prob"] < 1.0
        assert r["content_hash"]
    # Every horizon of one fire carries a DIFFERENT claim, so a different hash.
    assert len({r["content_hash"] for r in rows}) == len(rows)


@pytest.mark.asyncio
async def test_writer_uses_the_committed_morning_vol_basis(monkeypatch):
    """Anchoring to the immutable morning implied_move rather than a fresh
    VIX read is what keeps the intraday cone and the daily band on one basis."""
    mod = _reload("intraday_cone_writer")
    fake = _fake_writer_db(morning={"implied_move": Decimal("9.00")})
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    await mod._run(_writer_args(mod))
    wide = fake.insert_intraday_cone.await_args.args[0]

    fake2 = _fake_writer_db(morning={"implied_move": Decimal("2.00")})
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake2)
    await mod._run(_writer_args(mod))
    tight = fake2.insert_intraday_cone.await_args.args[0]

    assert (wide[0]["band_high"] - wide[0]["band_low"]) > (
        tight[0]["band_high"] - tight[0]["band_low"]
    )


@pytest.mark.asyncio
async def test_writer_still_fires_without_a_morning_commitment(monkeypatch, caplog):
    """A missed 08:30 writer must not silently cost a whole session of cones —
    the realized-so-far basis still produces an honest claim."""
    mod = _reload("intraday_cone_writer")
    fake = _fake_writer_db(morning={})
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_writer_args(mod)) == 0
    fake.insert_intraday_cone.assert_awaited_once()
    assert "realized-only vol basis" in caplog.text


@pytest.mark.asyncio
async def test_writer_drops_gamma_conditioning_on_a_stale_surface(monkeypatch, caplog):
    """A stale GEX read must not be presented as a live one.  Dropping the
    conditioning yields a wider, honest band instead of a confident band
    conditioned on positioning that cannot be verified."""
    mod = _reload("intraday_cone_writer")
    stale = datetime(2026, 9, 21, 11, 0, tzinfo=ET) - timedelta(hours=3)
    fake = _fake_writer_db(gex_ts=stale)
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_writer_args(mod)) == 0
    rows = fake.insert_intraday_cone.await_args.args[0]
    assert all(r["call_wall"] is None and r["gamma_flip"] is None for r in rows)
    assert all(r["net_gex_at_spot"] is None for r in rows)
    assert "stale" in caplog.text


@pytest.mark.asyncio
async def test_writer_skips_when_there_is_no_spot(monkeypatch, caplog):
    mod = _reload("intraday_cone_writer")
    fake = _fake_writer_db(quote=None, gex={"timestamp": None})
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_writer_args(mod)) == 0
    fake.insert_intraday_cone.assert_not_awaited()
    assert "no spot" in caplog.text


@pytest.mark.asyncio
async def test_writer_dry_run_writes_nothing(monkeypatch, caplog):
    mod = _reload("intraday_cone_writer")
    fake = _fake_writer_db()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_writer_args(mod, dry_run=True)) == 0
    fake.insert_intraday_cone.assert_not_awaited()
    assert "DRY RUN" in caplog.text


@pytest.mark.asyncio
async def test_writer_rerun_of_a_committed_fire_is_a_quiet_noop(monkeypatch, caplog):
    """The immutability contract surfaces as inserted==0, which is a log line
    and not an error — a retried timer must not look like a failure."""
    mod = _reload("intraday_cone_writer")
    fake = _fake_writer_db(inserted=0)
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_writer_args(mod)) == 0
    assert "already committed" in caplog.text


@pytest.mark.asyncio
async def test_writer_survives_a_dead_database(monkeypatch, caplog):
    mod = _reload("intraday_cone_writer")

    class _Broken:
        async def connect(self):
            raise RuntimeError("no route to host")

    monkeypatch.setattr(mod, "DatabaseManager", lambda: _Broken())
    assert await mod._run(_writer_args(mod)) == 0
    assert "DB connect failed" in caplog.text


@pytest.mark.asyncio
async def test_late_fire_publishes_only_horizons_that_fit(monkeypatch):
    mod = _reload("intraday_cone_writer")
    fake = _fake_writer_db()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_writer_args(mod, at="2026-09-21T15:00")) == 0
    rows = fake.insert_intraday_cone.await_args.args[0]
    assert [r["horizon_min"] for r in rows] == [30, 60]


# ---------------------------------------------------------------------------
# Grader
# ---------------------------------------------------------------------------


def _claim(**overrides):
    base = {
        "symbol": "SPY",
        "session_date": date(2026, 9, 21),
        "forecast_ts": datetime(2026, 9, 21, 11, 0, tzinfo=ET),
        "horizon_min": 30,
        "target_ts": datetime(2026, 9, 21, 11, 30, tzinfo=ET),
        "anchor_spot": Decimal("600.00"),
        "band_low": Decimal("598.00"),
        "band_high": Decimal("602.00"),
        "hold_prob": Decimal("0.7300"),
    }
    base.update(overrides)
    return base


def _fake_grader_db(*, due=None, extremes=None, wrote=True):
    db = type("FakeDB", (), {})()
    db.connect = AsyncMock(return_value=None)
    db.disconnect = AsyncMock(return_value=None)
    db.get_matured_ungraded_cones = AsyncMock(
        return_value=due if due is not None else [_claim()]
    )
    db.get_bar_extremes_between = AsyncMock(
        return_value=extremes
        if extremes is not None
        else {"window_low": Decimal("598.80"), "window_high": Decimal("601.40"), "bars": 30}
    )
    db.update_intraday_cone_receipt = AsyncMock(return_value=wrote)
    return db


def _grader_args(mod, **overrides):
    args = mod._parse_args([])
    args.at = "2026-09-21T11:35"
    args.dry_run = False
    for k, v in overrides.items():
        setattr(args, k, v)
    return args


@pytest.mark.asyncio
async def test_grader_reads_the_half_open_window(monkeypatch):
    """The anchor bar is excluded.  Spot sits inside its own band by
    construction, so including it could only ever flatter the verdict."""
    mod = _reload("intraday_cone_receipt")
    fake = _fake_grader_db()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    await mod._run(_grader_args(mod))

    symbol, start, end = fake.get_bar_extremes_between.await_args.args
    assert symbol == "SPY"
    assert start == datetime(2026, 9, 21, 11, 0, tzinfo=ET)
    assert end == datetime(2026, 9, 21, 11, 30, tzinfo=ET)


@pytest.mark.asyncio
async def test_grader_scores_a_contained_window_as_held(monkeypatch):
    mod = _reload("intraday_cone_receipt")
    fake = _fake_grader_db()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_grader_args(mod)) == 0

    kw = fake.update_intraday_cone_receipt.await_args.kwargs
    assert kw["held"] is True
    assert kw["brier"] == pytest.approx((0.73 - 1.0) ** 2, abs=1e-9)


@pytest.mark.asyncio
async def test_a_pierce_that_recovers_is_graded_as_broken(monkeypatch):
    """The whole reason the published number is a path-containment
    probability: price left the band and came back, and that is not a hold."""
    mod = _reload("intraday_cone_receipt")
    fake = _fake_grader_db(
        extremes={"window_low": Decimal("597.10"), "window_high": Decimal("601.00"), "bars": 30}
    )
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    await mod._run(_grader_args(mod))

    kw = fake.update_intraday_cone_receipt.await_args.kwargs
    assert kw["held"] is False
    assert kw["brier"] == pytest.approx(0.73**2, abs=1e-9)


@pytest.mark.asyncio
async def test_grader_leaves_a_recent_gap_pending(monkeypatch):
    """No bars yet is not a verdict.  The claim stays pending and is retried."""
    mod = _reload("intraday_cone_receipt")
    fake = _fake_grader_db(extremes=None)
    fake.get_bar_extremes_between = AsyncMock(return_value=None)
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_grader_args(mod)) == 0
    fake.update_intraday_cone_receipt.assert_not_awaited()


@pytest.mark.asyncio
async def test_grader_abandons_a_permanently_ungradeable_claim(monkeypatch, caplog):
    """A window that never produced bars is abandoned into a THIRD state —
    graded_at set, held NULL — so it leaves the pending queue without being
    deleted and without being counted as a win."""
    mod = _reload("intraday_cone_receipt")
    fake = _fake_grader_db()
    fake.get_bar_extremes_between = AsyncMock(return_value=None)
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    # Well past ABANDON_AFTER.
    assert await mod._run(_grader_args(mod, at="2026-09-30T11:35")) == 0

    kw = fake.update_intraday_cone_receipt.await_args.kwargs
    assert kw["held"] is None
    assert kw["brier"] is None
    assert kw["window_low"] is None
    assert kw["graded_at"] is not None
    assert "abandoning" in caplog.text
    assert "not scored" in caplog.text


@pytest.mark.asyncio
async def test_grader_dry_run_writes_nothing(monkeypatch, caplog):
    mod = _reload("intraday_cone_receipt")
    fake = _fake_grader_db()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_grader_args(mod, dry_run=True)) == 0
    fake.update_intraday_cone_receipt.assert_not_awaited()
    assert "DRY RUN" in caplog.text


@pytest.mark.asyncio
async def test_grader_tolerates_an_already_graded_row(monkeypatch):
    """A concurrent run getting there first is harmless, not a double-count."""
    mod = _reload("intraday_cone_receipt")
    fake = _fake_grader_db(wrote=False)
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_grader_args(mod)) == 0


@pytest.mark.asyncio
async def test_one_bad_claim_does_not_abort_the_run(monkeypatch, caplog):
    mod = _reload("intraday_cone_receipt")
    good = _claim(horizon_min=60, target_ts=datetime(2026, 9, 21, 12, 0, tzinfo=ET))
    fake = _fake_grader_db(due=[_claim(band_low=None), good])
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_grader_args(mod, at="2026-09-21T12:05")) == 0
    # The healthy claim still got graded.
    assert fake.update_intraday_cone_receipt.await_count == 1
    assert "failed" in caplog.text


@pytest.mark.asyncio
async def test_grader_with_nothing_due_is_quiet(monkeypatch, caplog):
    mod = _reload("intraday_cone_receipt")
    fake = _fake_grader_db(due=[])
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_grader_args(mod)) == 0
    assert "nothing matured" in caplog.text


@pytest.mark.asyncio
async def test_grader_survives_a_dead_database(monkeypatch, caplog):
    mod = _reload("intraday_cone_receipt")

    class _Broken:
        async def connect(self):
            raise RuntimeError("connection refused")

    monkeypatch.setattr(mod, "DatabaseManager", lambda: _Broken())
    assert await mod._run(_grader_args(mod)) == 0
    assert "DB connect failed" in caplog.text
