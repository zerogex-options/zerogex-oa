"""Model B — Aggressor-Inferred MM positioning.

The arm exists to measure one assumption: *the aggressor is a customer, so the
passive side is a market maker*.  These tests pin the mechanics that
assumption is built on so the measurement is of the assumption and not of a
bug: the side mapping (independent of option type), the session reset, the
causal replay, the production anchor, and the interval grid the attribution
comparison joins on.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import pytest

from research.mm_attributed_gex.aggressor import (
    ANCHORED_REASON,
    FLOW_ONLY_REASON,
    SOURCE_FLOW_FACTS,
    AggressorBucket,
    AggressorFlowBook,
    AggressorGateConfig,
    AggressorTimeline,
    aggregate_to_interval,
    assumed_mm_delta,
    coverage_by_session,
    interval_bucket_end,
    option_root_from_symbol,
    production_anchored_contracts,
    read_aggressor_jsonl,
    write_aggressor_jsonl,
)
from research.mm_attributed_gex.gex import (
    ChainQuote,
    build_engine,
    compute_mm_gex,
    production_profile_rows,
)
from research.mm_attributed_gex.schema import Interval
from research.mm_attributed_gex.walls import build_strike_structure

try:
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    import pytz

    ET = pytz.timezone("US/Eastern")

SYMBOL = "SPX"
SESSION = date(2026, 6, 15)  # Monday
NEXT_SESSION = date(2026, 6, 16)
EXPIRATION = date(2026, 6, 19)


def _et(d: date, hh: int, mm: int = 0, ss: int = 0) -> datetime:
    return datetime.combine(d, time(hh, mm, ss)).replace(tzinfo=ET).astimezone(timezone.utc)


def _bucket(
    hh: int,
    mm: int,
    *,
    buyer: int = 0,
    seller: int = 0,
    unknown: int = 0,
    strike: float = 6000.0,
    option_type: str = "C",
    session: date = SESSION,
    gamma: float | None = 0.001,
    **kw,
) -> AggressorBucket:
    return AggressorBucket(
        symbol=SYMBOL,
        option_symbol=f"SPXW {EXPIRATION:%y%m%d}{option_type}{int(strike):08d}",
        expiration=EXPIRATION,
        strike=strike,
        option_type=option_type,
        timestamp=_et(session, hh, mm),
        trading_date=session,
        buyer_initiated=buyer,
        seller_initiated=seller,
        unclassified=unknown,
        gamma=gamma,
        **kw,
    )


def _quote(strike: float, option_type: str, oi: int = 1_000, iv: float = 0.15) -> ChainQuote:
    return ChainQuote(
        option_symbol=f"SPXW {EXPIRATION:%y%m%d}{option_type}{int(strike):08d}",
        strike=strike,
        expiration=EXPIRATION,
        option_type=option_type,
        implied_volatility=iv,
        gamma=None,
        open_interest=oi,
        volume=100,
    )


# ---------------------------------------------------------------------------
# The side mapping — the assumption under test, and nothing else
# ---------------------------------------------------------------------------


def test_buyer_initiated_volume_is_an_assumed_mm_sale():
    assert assumed_mm_delta(buyer_initiated=100, seller_initiated=0) == -100.0


def test_seller_initiated_volume_is_an_assumed_mm_purchase():
    assert assumed_mm_delta(buyer_initiated=0, seller_initiated=100) == +100.0


def test_unclassified_volume_invents_no_side():
    b = _bucket(10, 0, unknown=500)
    assert b.assumed_mm_delta == 0.0
    book = AggressorFlowBook(SYMBOL).consume([b])
    assert book.positions() == []  # no signed change, nothing to price


@pytest.mark.parametrize("option_type", ["C", "P"])
def test_option_type_never_reverses_the_side_mapping(option_type):
    """A long put and a long call both have positive gamma; the MM quantity
    sign comes from the assumed buy/sell side, not from the contract type."""
    book = AggressorFlowBook(SYMBOL).consume(
        [
            _bucket(10, 0, buyer=300, option_type=option_type),
            _bucket(10, 1, seller=100, option_type=option_type),
        ]
    )
    (pos,) = book.positions()
    assert pos.option_type == option_type
    assert pos.net_contracts == -200.0  # 100 assumed buys − 300 assumed sells
    assert pos.long_contracts == 100.0 and pos.short_contracts == 300.0
    assert pos.left_censored is False
    assert pos.left_censor_reason == FLOW_ONLY_REASON


def test_option_root_is_parsed_from_the_symbol():
    assert option_root_from_symbol("SPXW 260619C06000000", "SPX") == "SPXW"
    assert option_root_from_symbol("SPX 260619C06000000", "SPX") == "SPX"
    assert option_root_from_symbol("", "SPX") == "SPX"


# ---------------------------------------------------------------------------
# Session scoping
# ---------------------------------------------------------------------------


def test_book_starts_every_session_from_zero():
    book = AggressorFlowBook(SYMBOL)
    book.consume([_bucket(10, 0, seller=400)])
    assert book.positions()[0].net_contracts == 400.0
    book.consume([_bucket(10, 0, seller=50, session=NEXT_SESSION)])
    (pos,) = book.positions()
    assert pos.net_contracts == 50.0, "previous session's flow must not carry over"
    assert book.session_date == NEXT_SESSION


def test_book_refuses_out_of_order_sessions():
    book = AggressorFlowBook(SYMBOL).consume([_bucket(10, 0, seller=1, session=NEXT_SESSION)])
    with pytest.raises(ValueError):
        book.consume([_bucket(10, 0, seller=1, session=SESSION)])


def test_replay_never_uses_a_bucket_known_after_the_snapshot():
    timeline = AggressorTimeline(SYMBOL)
    stamps = [_et(SESSION, 10, 0), _et(SESSION, 10, 5)]
    buckets = [_bucket(10, 0, seller=100), _bucket(10, 3, seller=50), _bucket(10, 7, buyer=999)]
    out = {ts: positions for ts, positions, _cov in timeline.replay(buckets, stamps)}
    # Exactly-on-the-stamp is known at the stamp; later minutes are not.
    assert out[_et(SESSION, 10, 0)][0].net_contracts == 100.0
    assert out[_et(SESSION, 10, 5)][0].net_contracts == 150.0


def test_replay_reports_nothing_before_the_first_bucket_of_a_new_session():
    """A 09:35 snapshot on day two must not see day one's inventory."""
    timeline = AggressorTimeline(SYMBOL)
    buckets = [_bucket(15, 0, seller=800), _bucket(9, 40, seller=10, session=NEXT_SESSION)]
    stamps = [_et(SESSION, 15, 30), _et(NEXT_SESSION, 9, 35), _et(NEXT_SESSION, 9, 45)]
    out = {ts: positions for ts, positions, _cov in timeline.replay(buckets, stamps)}
    assert out[_et(SESSION, 15, 30)][0].net_contracts == 800.0
    assert out[_et(NEXT_SESSION, 9, 35)] == []
    assert out[_et(NEXT_SESSION, 9, 45)][0].net_contracts == 10.0


def test_replay_drops_series_that_have_settled():
    timeline = AggressorTimeline(SYMBOL)
    after_settlement = _et(EXPIRATION, 16, 30)
    buckets = [_bucket(10, 0, seller=100, session=EXPIRATION)]
    out = {ts: positions for ts, positions, _ in timeline.replay(buckets, [after_settlement])}
    assert out[after_settlement] == []


# ---------------------------------------------------------------------------
# Coverage gates
# ---------------------------------------------------------------------------


def test_session_coverage_and_gate():
    buckets = [_bucket(10, i, buyer=10, seller=5, unknown=85, strike=6000.0 + i) for i in range(60)]
    cov = coverage_by_session(buckets)[SESSION]
    assert cov.buckets == 60 and len(cov.series) == 60
    assert cov.classified_share == pytest.approx(0.15)
    passed, reasons = cov.gate(AggressorGateConfig())
    assert passed is False and any("classified share" in r for r in reasons)
    passed, reasons = cov.gate(AggressorGateConfig(min_classified_share=0.1))
    assert passed is True and reasons == []


def test_extrapolated_source_fails_the_gate_unless_allowed():
    buckets = [
        _bucket(
            10,
            i,
            buyer=50,
            seller=50,
            strike=6000.0 + i,
            source=SOURCE_FLOW_FACTS,
            extrapolated=True,
        )
        for i in range(60)
    ]
    cov = coverage_by_session(buckets)[SESSION]
    assert cov.extrapolated is True
    assert cov.gate(AggressorGateConfig())[0] is False
    assert cov.gate(AggressorGateConfig(allow_extrapolated=True))[0] is True


# ---------------------------------------------------------------------------
# B2 — production anchor
# ---------------------------------------------------------------------------


def test_anchor_reduces_to_the_production_convention_without_flow():
    chain = [_quote(6000.0, "C", oi=800), _quote(5900.0, "P", oi=600)]
    contracts, diag = production_anchored_contracts(chain, [])
    by_key = {(c.strike, c.option_type): c.net_contracts for c in contracts}
    assert by_key == {(6000.0, "C"): +800.0, (5900.0, "P"): -600.0}
    assert all(c.position.left_censor_reason == ANCHORED_REASON for c in contracts)
    assert diag.anchored_contracts == 2 and diag.flow_series == 0


def test_anchor_adds_the_inferred_change_to_the_production_quantity():
    chain = [_quote(6000.0, "C", oi=800), _quote(5900.0, "P", oi=600), _quote(6100.0, "C", oi=0)]
    book = AggressorFlowBook(SYMBOL).consume(
        [
            _bucket(10, 0, buyer=300, strike=6000.0, option_type="C"),  # MM assumed sold 300
            _bucket(10, 1, seller=150, strike=5900.0, option_type="P"),  # MM assumed bought 150
            _bucket(10, 2, seller=40, strike=6100.0, option_type="C"),  # no OI anchor
            _bucket(10, 3, seller=70, strike=7000.0, option_type="C"),  # not in the chain
        ]
    )
    contracts, diag = production_anchored_contracts(chain, book.positions())
    by_key = {(c.strike, c.option_type): c.net_contracts for c in contracts}
    assert by_key[(6000.0, "C")] == 800.0 - 300.0
    assert by_key[(5900.0, "P")] == -600.0 + 150.0
    assert by_key[(6100.0, "C")] == 40.0  # anchor 0, flow only
    assert diag.flow_series == 4
    assert diag.flow_series_matched == 3
    assert diag.flow_series_unmatched == 1
    assert diag.flow_contracts_unmatched == 70.0


def test_anchor_without_flow_reproduces_production_readings_exactly():
    """B2 with no classified volume must equal Model A — same kernels, same rows.

    This is the parity that makes the three-arm comparison a comparison of
    positioning and nothing else: if the anchor drifted from production, every
    B2 difference would be partly arithmetic.
    """
    from src.analytics.walls import compute_call_put_walls_with_strength

    engine = build_engine(SYMBOL)
    spot, ts = 6000.0, _et(SESSION, 14, 0)
    chain = [
        _quote(5900.0, "P", oi=1_500),
        _quote(5950.0, "P", oi=900),
        _quote(6000.0, "C", oi=1_200),
        _quote(6050.0, "C", oi=700),
        _quote(6100.0, "C", oi=400),
    ]
    # Production, recomputed the way the dataset builder recomputes it.
    rows = production_profile_rows(chain)
    profile, flip, _span = engine._resolve_gamma_flip(rows, spot, ts)
    a_at_spot = engine._net_gex_at_spot(profile, spot)
    a_strikes = engine._calculate_gex_by_strike(rows, spot, ts, recompute_gamma=True)
    a_walls = compute_call_put_walls_with_strength(a_strikes, spot)

    contracts, _diag = production_anchored_contracts(chain, [])
    b2 = compute_mm_gex(contracts, spot, ts, engine=engine)
    struct = build_strike_structure(b2.strike_rows, spot)

    assert b2.mm_attributed_gamma_at_spot == pytest.approx(a_at_spot, rel=1e-12)
    assert b2.mm_attributed_gamma_flip == pytest.approx(flip, rel=1e-12)
    assert b2.mm_attributed_net_gex == pytest.approx(
        sum(r["net_gex"] for r in a_strikes), rel=1e-12
    )
    assert (struct.definition_a.call_wall, struct.definition_a.put_wall) == (a_walls[0], a_walls[1])
    assert struct.definition_a.call_wall_strength == pytest.approx(a_walls[2], rel=1e-12)
    assert struct.definition_a.put_wall_strength == pytest.approx(a_walls[3], rel=1e-12)


# ---------------------------------------------------------------------------
# Interval grid for the attribution comparison
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("session", [date(2026, 1, 12), date(2026, 7, 13)])  # EST and EDT
def test_ten_minute_bucket_ends_follow_the_et_wall_clock_across_dst(session):
    end = interval_bucket_end(_et(session, 9, 31), Interval.MINUTE_10, session)
    assert end.astimezone(ET).time() == time(9, 40)
    on_boundary = interval_bucket_end(_et(session, 9, 40), Interval.MINUTE_10, session)
    assert on_boundary.astimezone(ET).time() == time(9, 40)
    one_second_after = interval_bucket_end(_et(session, 9, 40, 1), Interval.MINUTE_10, session)
    assert one_second_after.astimezone(ET).time() == time(9, 50)


def test_session_buckets_are_stamped_at_the_close_like_the_cboe_loader():
    end = interval_bucket_end(_et(SESSION, 10, 0), Interval.SESSION, SESSION)
    assert end.astimezone(ET).time() == time(16, 0)


def test_aggregation_sums_within_a_bucket_and_never_leaks_backward():
    cells = aggregate_to_interval(
        [_bucket(9, 33, buyer=10), _bucket(9, 40, seller=5), _bucket(9, 41, buyer=7)],
        Interval.MINUTE_10,
    )
    ends = sorted(k[0].astimezone(ET).time() for k in cells)
    assert ends == [time(9, 40), time(9, 50)]
    first = next(v for k, v in cells.items() if k[0].astimezone(ET).time() == time(9, 40))
    assert (first.buyer_initiated, first.seller_initiated, first.rows) == (10.0, 5.0, 2)
    assert first.signed == -5.0
    later = next(v for k, v in cells.items() if k[0].astimezone(ET).time() == time(9, 50))
    assert later.buyer_initiated == 7.0  # the 09:41 row cannot inform the 09:40 bucket


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def test_jsonl_round_trip_preserves_every_field(tmp_path):
    buckets = [
        _bucket(10, 0, buyer=3, seller=4, unknown=5, quote_locked=True, first_row_of_session=True),
        _bucket(10, 1, buyer=1, option_type="P", gamma=None, implied_volatility=0.2),
    ]
    path = write_aggressor_jsonl(buckets, tmp_path / "b.jsonl")
    loaded = list(read_aggressor_jsonl(path))
    assert loaded == buckets


def test_bucket_rejects_negative_counts_and_naive_timestamps():
    with pytest.raises(ValueError):
        _bucket(10, 0, buyer=-1)
    with pytest.raises(ValueError):
        AggressorBucket(
            symbol=SYMBOL,
            option_symbol="SPXW 260619C06000000",
            expiration=EXPIRATION,
            strike=6000.0,
            option_type="C",
            timestamp=datetime(2026, 6, 15, 14, 0),
            trading_date=SESSION,
            buyer_initiated=1,
            seller_initiated=0,
            unclassified=0,
        )


# ---------------------------------------------------------------------------
# Database reader (fake cursor — the SQL never runs here)
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, rows, log):
        self._rows = rows
        self._log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._log.append((sql, params))

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.log: list = []

    def cursor(self):
        return _FakeCursor(self.rows, self.log)


def test_option_chains_reader_shifts_stamps_to_the_known_at_instant_and_maps_fields():
    from research.mm_attributed_gex.sources import fetch_aggressor_buckets

    row_ts = _et(SESSION, 10, 0)  # bucket START stored by ingestion
    conn = _FakeConn(
        [
            (
                "SPXW 260619C06000000",
                row_ts,
                6000.0,
                EXPIRATION,
                "C",
                30,
                5,
                10,
                True,
                2.5,
                2.5,
                0.001,
                0.15,
            ),
            (
                "SPXW 260619P05900000",
                row_ts,
                5900.0,
                EXPIRATION,
                "P",
                0,
                0,
                7,
                False,
                5.1,
                4.9,
                None,
                None,
            ),
        ]
    )
    out = list(fetch_aggressor_buckets(conn, SYMBOL, [SESSION]))
    assert len(out) == 2
    call, put = out
    assert call.timestamp == row_ts + timedelta(minutes=1)
    assert (call.buyer_initiated, call.unclassified, call.seller_initiated) == (30, 5, 10)
    assert call.first_row_of_session is True and call.quote_locked is True
    assert call.gamma == 0.001 and call.implied_volatility == 0.15
    assert put.quote_crossed is True and put.gamma is None
    assert put.source == "option_chains" and put.extrapolated is False
    # The session bounds handed to SQL are 09:30 and 16:15 ET in UTC.
    _sql, params = conn.log[0]
    assert params["session_open"] == _et(SESSION, 9, 30)
    assert params["session_end"] == _et(SESSION, 16, 15)
    assert params["symbol"] == SYMBOL


def test_flow_facts_reader_labels_every_bucket_extrapolated():
    from research.mm_attributed_gex.sources import fetch_aggressor_buckets_from_facts

    row_ts = _et(SESSION, 10, 0)
    conn = _FakeConn([("SPXW 260619C06000000", row_ts, 6000.0, EXPIRATION, "C", 40, 25, 15, 0.2)])
    (b,) = list(fetch_aggressor_buckets_from_facts(conn, SYMBOL, [SESSION]))
    assert (b.buyer_initiated, b.seller_initiated, b.unclassified) == (25, 15, 0)
    assert b.extrapolated is True and b.source == "flow_contract_facts"
    assert b.timestamp == row_ts + timedelta(minutes=1)
