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
    walk=None,
    trailing=None,
):
    """A stubbed DB whose reads are POINT-IN-TIME, like the real ones.

    ``walk`` maps an ET "HH:MM" anchor to the price printing then, so a test
    can assert the writer actually re-anchors instead of reusing one quote.
    When it is None every anchor sees the same ``quote``, which is the shape
    most tests want.
    """
    now = datetime(2026, 9, 21, 11, 0, tzinfo=ET)
    if gex is None:
        gex = {
            "call_wall": Decimal("606.00"),
            "put_wall": Decimal("594.00"),
            "gamma_flip": Decimal("601.00"),
            "net_gex_at_spot": Decimal("400000000"),
            "timestamp": gex_ts if gex_ts is not None else now - timedelta(minutes=1),
        }

    async def _quote_as_of(symbol, as_of, not_before):
        if quote is None:
            return None
        if as_of < not_before:
            return None
        price = quote
        if walk is not None:
            key = as_of.strftime("%H:%M")
            if key not in walk:
                return None
            price = walk[key]
        return {"timestamp": as_of, "close": Decimal(str(price))}

    db = type("FakeDB", (), {})()
    db.connect = AsyncMock(return_value=None)
    db.disconnect = AsyncMock(return_value=None)
    db.get_quote_as_of = AsyncMock(side_effect=_quote_as_of)
    db.get_gex_summary_as_of = AsyncMock(return_value=gex)
    db.get_daily_forecast = AsyncMock(
        return_value=morning if morning is not None else {
            "implied_move": Decimal("4.50"),
            "forecast_inputs": {"calibration_applied": {"vol_range_basis_mult": 0.65}},
        }
    )
    db.get_bar_extremes_between = AsyncMock(
        return_value=extremes
        if extremes is not None
        else {"window_low": Decimal("598.50"), "window_high": Decimal("602.00"), "bars": 90}
    )
    db.get_trailing_realized_vol_ratios = AsyncMock(
        return_value=list(trailing) if trailing is not None else []
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
    assert "no SPY bar at or before" in caplog.text


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


@pytest.mark.asyncio
async def test_a_backfill_re_anchors_on_every_fire(monkeypatch):
    """The bug that made the first real backfill meaningless.

    The writer read "the newest quote" rather than "the quote as of this
    anchor", so a Sunday run of Friday's session got Friday's CLOSING print
    for all 25 fires: every band was drawn around the same price, the cone
    that re-anchors every 15 minutes never re-anchored, and grading those
    bands against a tape that was somewhere else all day produced a 23% hold
    rate against a model predicting 50-81%.

    The giveaway was visible in the log and nowhere in the tests: an
    identical ``spot=`` on every line. This pins it.
    """
    mod = _reload("intraday_cone_writer")
    walk = {"09:45": 600.0, "11:00": 604.5, "14:00": 597.25}
    anchors = []
    for at, expected in walk.items():
        fake = _fake_writer_db(walk=walk)
        monkeypatch.setattr(mod, "DatabaseManager", lambda f=fake: f)
        assert await mod._run(_writer_args(mod, at=f"2026-09-21T{at}")) == 0
        rows = fake.insert_intraday_cone.await_args.args[0]
        got = float(rows[0]["anchor_spot"])
        assert got == expected, f"{at} anchored at {got}, expected {expected}"
        anchors.append(got)

    assert len(set(anchors)) == len(anchors), "every fire must anchor on its own price"


@pytest.mark.asyncio
async def test_the_quote_read_is_bounded_to_the_anchors_own_session(monkeypatch):
    """A point-in-time read still has to refuse to reach backwards forever.

    Without a lower bound, a session with no bars anchors the cone on some
    previous day's close — and that cone is still graded, which is worse than
    no cone at all.
    """
    mod = _reload("intraday_cone_writer")
    fake = _fake_writer_db()
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    await mod._run(_writer_args(mod))

    _symbol, as_of, not_before = fake.get_quote_as_of.await_args.args
    assert as_of == datetime(2026, 9, 21, 11, 0, tzinfo=ET)
    assert not_before == datetime(2026, 9, 21, 9, 30, tzinfo=ET), (
        "the lookback must stop at the session open"
    )


@pytest.mark.asyncio
async def test_a_surface_from_the_future_is_not_fresh(monkeypatch, caplog):
    """The lookahead hole, and the reason it survived review.

    ``(now - ts) <= GEX_MAX_STALENESS`` is True for a NEGATIVE age, so a
    snapshot timestamped AFTER the anchor passed the freshness check. During
    a backfill that is not a corner case, it is the normal case: the cone
    would be conditioned on walls that did not exist when the claim was made.
    Lookahead in the one system whose value is honest grading invalidates
    every number it publishes, so a future-dated surface is rejected outright.
    """
    mod = _reload("intraday_cone_writer")
    future = datetime(2026, 9, 23, 11, 0, tzinfo=ET)   # two days AFTER the anchor
    fake = _fake_writer_db(gex_ts=future)
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_writer_args(mod)) == 0

    rows = fake.insert_intraday_cone.await_args.args[0]
    assert all(r["call_wall"] is None for r in rows), "future walls must not condition the cone"
    assert all(r["gamma_flip"] is None for r in rows)
    assert all(r["net_gex_at_spot"] is None for r in rows)
    assert "stale" in caplog.text


def test_freshness_rejects_negative_age_directly():
    """The guard itself, independent of the job wiring."""
    mod = _reload("intraday_cone_writer")
    now = datetime(2026, 9, 21, 11, 0, tzinfo=ET)
    assert mod._gex_is_fresh(now - timedelta(minutes=1), now) is True
    assert mod._gex_is_fresh(now, now) is True
    assert mod._gex_is_fresh(now - timedelta(hours=3), now) is False, "too old"
    assert mod._gex_is_fresh(now + timedelta(minutes=1), now) is False, "from the future"
    assert mod._gex_is_fresh(now + timedelta(days=2), now) is False
    assert mod._gex_is_fresh(None, now) is False


@pytest.mark.asyncio
async def test_a_session_with_no_bars_commits_nothing(monkeypatch, caplog):
    mod = _reload("intraday_cone_writer")
    fake = _fake_writer_db(walk={})     # no bar at any anchor
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_writer_args(mod)) == 0
    fake.insert_intraday_cone.assert_not_awaited()
    assert "no SPY bar at or before" in caplog.text


@pytest.mark.asyncio
async def test_the_trailing_vol_read_cannot_see_its_own_future(monkeypatch):
    """The lookahead trap, arriving by a different door.

    The obvious way to get trailing realized ratios is
    get_daily_forecast_history, which takes no date bound and returns the
    newest rows in the table. During a backfill those are sessions AFTER the
    one being reconstructed. The date bound is the whole point, so it is
    asserted on the call itself rather than inferred from the output.
    """
    mod = _reload("intraday_cone_writer")
    fake = _fake_writer_db(trailing=[0.5, 0.6, 0.55, 0.48, 0.52, 0.6])
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_writer_args(mod)) == 0

    symbol, before, limit = fake.get_trailing_realized_vol_ratios.await_args.args
    assert symbol == "SPY"
    assert before == date(2026, 9, 21), "the bound must be the session being written"
    assert limit == 10


@pytest.mark.asyncio
async def test_a_measured_anchor_is_preferred_over_a_predicted_one(monkeypatch):
    """Graded outcomes beat a forecast of them — especially this forecast,
    which degrades to a neutral 1.0 on a cold start and so silently reasserts
    'today is average', the error the anchor exists to correct."""
    mod = _reload("intraday_cone_writer")
    measured = _fake_writer_db(
        trailing=[0.5, 0.52, 0.48, 0.55, 0.5, 0.51],
        morning={"implied_move": Decimal("4.50"), "expected_vol_ratio": Decimal("1.00")},
    )
    monkeypatch.setattr(mod, "DatabaseManager", lambda: measured)
    await mod._run(_writer_args(mod))
    rows = measured.insert_intraday_cone.await_args.args[0]
    assert rows[0]["vol_ratio_source"] == "measured"
    assert rows[0]["vol_ratio_applied"] == pytest.approx(0.505, abs=0.02)

    # Cold start: fewer than the minimum observations, so the prediction is
    # all that is left.
    cold = _fake_writer_db(
        trailing=[0.5, 0.52],
        morning={"implied_move": Decimal("4.50"), "expected_vol_ratio": Decimal("0.80")},
    )
    monkeypatch.setattr(mod, "DatabaseManager", lambda: cold)
    await mod._run(_writer_args(mod))
    cold_rows = cold.insert_intraday_cone.await_args.args[0]
    assert cold_rows[0]["vol_ratio_source"] == "committed"
    assert cold_rows[0]["vol_ratio_applied"] == pytest.approx(0.80, abs=1e-6)


@pytest.mark.asyncio
async def test_with_neither_anchor_the_cone_makes_no_vol_claim(monkeypatch):
    """Honestly wide beats confidently wrong."""
    mod = _reload("intraday_cone_writer")
    fake = _fake_writer_db(trailing=[], morning={"implied_move": Decimal("4.50")})
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    await mod._run(_writer_args(mod))
    rows = fake.insert_intraday_cone.await_args.args[0]
    assert rows[0]["vol_ratio_source"] == "none"
    assert rows[0]["vol_ratio_applied"] is None


@pytest.mark.asyncio
async def test_a_measured_anchor_actually_narrows_the_bands(monkeypatch):
    """The effect that was missing the first time round: adopting a vol call
    of ~0.5 has to move the basis by about 40%, not 12%."""
    mod = _reload("intraday_cone_writer")
    flat = _fake_writer_db(trailing=[])
    monkeypatch.setattr(mod, "DatabaseManager", lambda: flat)
    await mod._run(_writer_args(mod))
    wide = flat.insert_intraday_cone.await_args.args[0][0]

    quiet = _fake_writer_db(trailing=[0.5, 0.52, 0.48, 0.55, 0.5, 0.51])
    monkeypatch.setattr(mod, "DatabaseManager", lambda: quiet)
    await mod._run(_writer_args(mod))
    tight = quiet.insert_intraday_cone.await_args.args[0][0]

    assert float(tight["daily_sigma"]) < float(wide["daily_sigma"])
    shrink = 1 - float(tight["daily_sigma"]) / float(wide["daily_sigma"])
    assert shrink > 0.20, f"the anchor must move the basis materially, got {shrink:.0%}"


@pytest.mark.asyncio
async def test_a_failing_trailing_read_does_not_stop_the_fire(monkeypatch, caplog):
    mod = _reload("intraday_cone_writer")
    fake = _fake_writer_db()
    fake.get_trailing_realized_vol_ratios = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_writer_args(mod)) == 0
    fake.insert_intraday_cone.assert_awaited_once()
    assert "trailing vol ratios failed" in caplog.text


@pytest.mark.asyncio
async def test_a_failed_write_is_never_reported_as_already_committed(monkeypatch, caplog):
    """The bug that hid a whole missing session.

    insert_intraday_cone used to return 0 on an exception as well as on a
    clean conflict, so a writer whose every insert was rejected by a missing
    column logged "already committed" and exited 0. The backfill looked like a
    healthy idempotent re-run and the session simply was not there. A job that
    announces success when it failed is worse than one that crashes.
    """
    mod = _reload("intraday_cone_writer")
    fake = _fake_writer_db(inserted=None)     # None == the write failed
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_writer_args(mod)) == 0

    assert "FAILED to commit" in caplog.text
    assert "claims lost" in caplog.text
    assert "already committed" not in caplog.text
    assert "failed to commit at" in caplog.text


@pytest.mark.asyncio
async def test_a_clean_conflict_is_still_a_quiet_noop(monkeypatch, caplog):
    """The other half: a genuine re-run must stay boring."""
    mod = _reload("intraday_cone_writer")
    fake = _fake_writer_db(inserted=0)
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_writer_args(mod)) == 0
    assert "already committed" in caplog.text
    assert "FAILED" not in caplog.text


@pytest.mark.asyncio
async def test_the_vol_basis_comes_from_the_committed_snapshot(monkeypatch):
    """Read from the morning row's immutable forecast_inputs, not from live
    calibration state — the live scalar is relearned nightly, so a backfill
    reading it would use a number derived from sessions after the one being
    rebuilt."""
    mod = _reload("intraday_cone_writer")
    wide = _fake_writer_db(morning={
        "implied_move": Decimal("4.50"),
        "forecast_inputs": {"calibration_applied": {"vol_range_basis_mult": 1.30}},
    })
    monkeypatch.setattr(mod, "DatabaseManager", lambda: wide)
    await mod._run(_writer_args(mod))
    wide_row = wide.insert_intraday_cone.await_args.args[0][0]

    tight = _fake_writer_db(morning={
        "implied_move": Decimal("4.50"),
        "forecast_inputs": {"calibration_applied": {"vol_range_basis_mult": 0.55}},
    })
    monkeypatch.setattr(mod, "DatabaseManager", lambda: tight)
    await mod._run(_writer_args(mod))
    tight_row = tight.insert_intraday_cone.await_args.args[0][0]

    assert float(tight_row["daily_sigma"]) < float(wide_row["daily_sigma"])


def test_the_committed_basis_survives_json_and_missing_shapes():
    """forecast_inputs is JSONB and may arrive as a string; anything else
    widens the cone rather than breaking the fire."""
    mod = _reload("intraday_cone_writer")
    assert mod._committed_vol_basis(
        {"forecast_inputs": '{"calibration_applied": {"vol_range_basis_mult": 0.7}}'}
    ) == pytest.approx(0.7)
    assert mod._committed_vol_basis(
        {"forecast_inputs": {"calibration_applied": {"vol_range_basis_mult": 0.7}}}
    ) == pytest.approx(0.7)
    for bad in ({}, {"forecast_inputs": None}, {"forecast_inputs": "not json"},
                {"forecast_inputs": {"calibration_applied": None}},
                {"forecast_inputs": {"calibration_applied": {}}},
                {"forecast_inputs": []}):
        assert mod._committed_vol_basis(bad) is None


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
    """A STATEFUL stand-in: graded claims leave the pending queue.

    The previous fixture returned a fixed page from every fetch, which is not
    how the real query behaves and is why a truncating grader looked healthy
    in tests while dropping a whole session in production.
    """
    pending = list(due if due is not None else [_claim()])

    def _key(r):
        return (r["symbol"], r["forecast_ts"], r["horizon_min"])

    async def _fetch(now, limit=2000):
        return [r for r in pending if r["target_ts"] <= now][:limit]

    async def _write(**kw):
        if not wrote:
            return False
        want = (kw["symbol"], kw["forecast_ts"], kw["horizon_min"])
        for r in list(pending):
            if _key(r) == want:
                pending.remove(r)
        return True

    db = type("FakeDB", (), {})()
    db.connect = AsyncMock(return_value=None)
    db.disconnect = AsyncMock(return_value=None)
    db.get_matured_ungraded_cones = AsyncMock(side_effect=_fetch)
    db.get_bar_extremes_between = AsyncMock(
        return_value=extremes
        if extremes is not None
        else {"window_low": Decimal("598.80"), "window_high": Decimal("601.40"), "bars": 30}
    )
    db.update_intraday_cone_receipt = AsyncMock(side_effect=_write)
    db._pending = pending
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
async def test_the_grader_drains_a_backlog_larger_than_one_page(monkeypatch, caplog):
    """The truncation that lost a whole session, three reports running.

    The fetch orders oldest-first and caps at --limit, and the first version
    graded exactly one page. A backlog above the cap therefore left the NEWEST
    session ungraded — and since the calibration report requires a verdict,
    that session vanished from the report entirely rather than showing up
    short. The tell was a run logging "500 matured" against a limit of 500.
    """
    mod = _reload("intraday_cone_receipt")
    backlog = [
        _claim(minute=m % 60, horizon=h)
        for m in range(0, 30)
        for h in (30, 60, 90, 120)
    ]
    # Distinct keys so the stateful fixture can remove them independently.
    for i, r in enumerate(backlog):
        r["forecast_ts"] = datetime(2026, 9, 21, 9, 45, tzinfo=ET) + timedelta(minutes=i)
        r["target_ts"] = r["forecast_ts"] + timedelta(minutes=r["horizon_min"])

    fake = _fake_grader_db(due=backlog)
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_grader_args(mod, at="2026-09-22T12:00")) == 0

    assert fake._pending == [], "every matured claim must be graded"
    assert fake.get_matured_ungraded_cones.await_count > 1, "it must page"
    assert "page(s)" in caplog.text


@pytest.mark.asyncio
async def test_a_page_that_cannot_progress_stops_instead_of_spinning(monkeypatch):
    """Rows waiting on bars come back on every fetch. Counting them as
    progress would turn the cron into an infinite loop."""
    mod = _reload("intraday_cone_receipt")
    fake = _fake_grader_db()
    fake.get_bar_extremes_between = AsyncMock(return_value=None)   # always pending
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_grader_args(mod)) == 0
    assert fake.get_matured_ungraded_cones.await_count == 1


@pytest.mark.asyncio
async def test_a_row_that_always_errors_does_not_spin_the_loop(monkeypatch):
    """An error is not a departure either — the row is still in the queue."""
    mod = _reload("intraday_cone_receipt")
    fake = _fake_grader_db(due=[_claim(band_low=None)])
    monkeypatch.setattr(mod, "DatabaseManager", lambda: fake)
    assert await mod._run(_grader_args(mod)) == 0
    assert fake.get_matured_ungraded_cones.await_count == 1


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
