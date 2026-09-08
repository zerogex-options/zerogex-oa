"""B-vs-C attribution comparison: hand-checkable cases.

Each case is small enough that the expected number can be worked out on paper.
The point is not coverage of the arithmetic but of the *decisions*: what counts
as a matched cell, what a zero means, what is excluded and counted rather than
silently zeroed, and that the active-cell floor does what it says.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone

import pytest

from research.mm_attributed_gex.aggressor import AggressorBucket
from research.mm_attributed_gex.attribution import (
    AttributionConfig,
    attribution_metrics,
    compare_attribution,
    match_cells,
    render_attribution_markdown,
    write_attribution_report,
)
from research.mm_attributed_gex.schema import (
    Exchange,
    Interval,
    ParticipantActivity,
    ParticipantType,
    PositionEffect,
    Side,
)

try:
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    import pytz

    ET = pytz.timezone("US/Eastern")

SYMBOL = "SPX"
SESSION = date(2026, 6, 15)
SESSION_2 = date(2026, 6, 16)
EXPIRATION = date(2026, 6, 19)


def _et(d: date, hh: int, mm: int = 0) -> datetime:
    return datetime.combine(d, time(hh, mm)).replace(tzinfo=ET).astimezone(timezone.utc)


def _b(
    hh,
    mm,
    *,
    buyer=0,
    seller=0,
    unknown=0,
    strike=6000.0,
    option_type="C",
    session=SESSION,
    gamma=0.001,
):
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
    )


def _c(
    hh,
    mm,
    contracts,
    *,
    side=Side.BUY,
    strike=6000.0,
    option_type="C",
    session=SESSION,
    participant=ParticipantType.MARKET_MAKER,
    interval=Interval.MINUTE_10,
    attrs=None,
):
    return ParticipantActivity(
        exchange=Exchange.CBOE_C1,
        symbol=SYMBOL,
        expiration=EXPIRATION,
        strike=strike,
        option_type=option_type,
        participant=participant,
        side=side,
        position_effect=PositionEffect.OPEN,
        contracts=contracts,
        timestamp=_et(session, hh, mm),
        trading_date=session,
        interval=interval,
        option_root="SPXW",
        attrs=attrs or {},
    )


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------


def test_perfect_agreement_scores_one():
    """Buyer-initiated 100 → assumed MM sold 100; the exchange says MM sold 100."""
    cells, diag = match_cells([_b(9, 35, buyer=100)], [_c(9, 40, 100, side=Side.SELL)])
    assert diag.interval == "10min" and diag.matched_cells == 1
    (cell,) = cells
    assert cell.b_signed == -100.0 and cell.c_signed == -100.0
    assert cell.sign_agreement is True and cell.abs_error == 0.0
    m = attribution_metrics(cells, active_threshold=10.0)
    assert m["active_cells"]["sign_agreement_both_nonzero"] == 1.0
    assert m["active_cells"]["mae"] == 0.0


def test_complete_disagreement_scores_zero():
    """Buyer-initiated 100, but the exchange says MM BOUGHT 100 (customer was passive)."""
    cells, _ = match_cells([_b(9, 35, buyer=100)], [_c(9, 40, 100, side=Side.BUY)])
    (cell,) = cells
    assert cell.sign_agreement is False
    assert cell.abs_error == 200.0
    m = attribution_metrics(cells, active_threshold=10.0)
    assert m["active_cells"]["sign_agreement_both_nonzero"] == 0.0
    assert m["active_cells"]["bias_mean_b_minus_c"] == -200.0


def test_attributed_zero_flow_cells_are_kept_but_reported_separately():
    """B sees flow in a bucket the exchange shows no MM activity in — a real zero,
    because the series-session is covered by both feeds."""
    b = [_b(9, 35, buyer=100), _b(9, 45, buyer=50)]
    c = [_c(9, 40, 100, side=Side.SELL)]  # nothing at 09:50
    cells, diag = match_cells(b, c)
    assert diag.matched_cells == 2 and diag.cells_b_only == 1
    m = attribution_metrics(cells, active_threshold=10.0)
    assert m["all_cells"]["n_c_zero"] == 1
    # Both-non-zero agreement ignores the zero cell; zero-as-sign penalises it.
    assert m["all_cells"]["sign_agreement_both_nonzero"] == 1.0
    assert m["all_cells"]["sign_agreement_zero_as_sign"] == 0.5
    # The active floor drops the zero cell entirely.
    assert m["active_cells"]["n"] == 1


def test_a_large_gamma_weighted_disagreement_dominates_the_weighted_rate():
    """Two small agreeing cells, one large disagreeing cell at 10× the gamma."""
    b = [
        _b(9, 35, buyer=10, strike=6100.0, gamma=0.0001),
        _b(9, 35, buyer=10, strike=6200.0, gamma=0.0001),
        _b(9, 35, seller=500, strike=6000.0, gamma=0.001),
    ]
    c = [
        _c(9, 40, 10, side=Side.SELL, strike=6100.0),
        _c(9, 40, 10, side=Side.SELL, strike=6200.0),
        _c(9, 40, 500, side=Side.SELL, strike=6000.0),  # exchange: MM sold; B assumed MM bought
    ]
    cells, _ = match_cells(b, c, spot_provider=lambda ts: 6000.0)
    m = attribution_metrics(cells, active_threshold=1.0)["active_cells"]
    assert m["sign_agreement_both_nonzero"] == pytest.approx(2 / 3)
    # weights: 10·1e-4·k, 10·1e-4·k, 500·1e-3·k  →  agreeing weight 0.002k of 0.502k
    assert m["weighted_sign_agreement_by_abs_c_gamma"] == pytest.approx(0.002 / 0.502)
    assert m["gamma_weighted_pearson"] is not None


def test_series_the_exchange_never_covers_are_excluded_not_zeroed():
    b = [_b(9, 35, buyer=100, strike=6000.0), _b(9, 35, buyer=70, strike=7000.0)]
    c = [_c(9, 40, 100, side=Side.SELL, strike=6000.0)]
    cells, diag = match_cells(b, c)
    assert diag.matched_cells == 1
    assert diag.series_days_b_only == 1
    assert diag.b_activity_excluded_no_c_coverage == 70.0


def test_series_the_tape_never_classifies_are_excluded_not_zeroed():
    b = [_b(9, 35, buyer=100, strike=6000.0)]
    c = [_c(9, 40, 100, side=Side.SELL, strike=6000.0), _c(9, 40, 40, side=Side.BUY, strike=5900.0)]
    cells, diag = match_cells(b, c)
    assert diag.matched_cells == 1
    assert diag.series_days_c_only == 1
    assert diag.c_activity_excluded_no_b_coverage == 40.0


def test_within_a_covered_series_session_a_missing_tape_bucket_is_a_real_zero():
    """Same series, same session: B quiet at 09:50 while the exchange shows MM activity."""
    b = [_b(9, 35, buyer=100)]
    c = [_c(9, 40, 100, side=Side.SELL), _c(9, 50, 30, side=Side.BUY)]
    cells, diag = match_cells(b, c)
    assert diag.matched_cells == 2 and diag.cells_c_only == 1
    quiet = next(x for x in cells if x.has_c and not x.has_b)
    assert quiet.b_signed == 0.0 and quiet.c_signed == 30.0


def test_active_floor_filters_on_attributed_gross_activity():
    b = [_b(9, 35, buyer=3), _b(9, 45, buyer=300)]
    c = [_c(9, 40, 3, side=Side.SELL), _c(9, 50, 300, side=Side.SELL)]
    cells, _ = match_cells(b, c)
    assert attribution_metrics(cells, active_threshold=10.0)["active_cells"]["n"] == 1
    assert attribution_metrics(cells, active_threshold=1.0)["active_cells"]["n"] == 2


def test_only_market_maker_records_enter_model_c():
    b = [_b(9, 35, buyer=100)]
    c = [
        _c(9, 40, 100, side=Side.SELL),
        _c(9, 40, 999, side=Side.BUY, participant=ParticipantType.CUSTOMER),
    ]
    cells, diag = match_cells(b, c)
    assert diag.c_records == 2 and diag.c_mm_records == 1
    assert cells[0].c_signed == -100.0


def test_session_summary_feed_disables_intraday_claims():
    b = [_b(10, 0, buyer=60), _b(14, 0, seller=20)]
    c = [_c(16, 0, 40, side=Side.SELL, interval=Interval.SESSION)]
    cells, diag = match_cells(b, c)
    assert diag.intraday_identification_testable is False
    assert diag.interval == "session" and diag.matched_cells == 1
    assert cells[0].b_signed == -40.0 and cells[0].c_signed == -40.0


def test_mixed_intervals_are_refused():
    with pytest.raises(ValueError):
        match_cells([_b(9, 35, buyer=1)], [_c(9, 40, 1), _c(9, 41, 1, interval=Interval.MINUTE_1)])


def test_a_minute_row_never_reaches_an_earlier_exchange_bucket():
    """09:41 tape volume must not inform the 09:40 exchange bucket."""
    cells, _ = match_cells(
        [_b(9, 41, buyer=100)], [_c(9, 40, 100, side=Side.SELL), _c(9, 50, 1, side=Side.SELL)]
    )
    by_end = {c.bucket_end.astimezone(ET).time(): c for c in cells}
    assert by_end[time(9, 40)].b_signed == 0.0
    assert by_end[time(9, 50)].b_signed == -100.0


# ---------------------------------------------------------------------------
# Strata, sensitivity, report
# ---------------------------------------------------------------------------


def _two_session_fixture():
    b, c = [], []
    for session in (SESSION, SESSION_2):
        for i in range(12):
            strike = 5900.0 + 25.0 * i
            option_type = "C" if i % 2 == 0 else "P"
            hh, mm = (9, 35) if i < 4 else ((12, 5) if i < 8 else (15, 35))
            end_hh, end_mm = (9, 40) if i < 4 else ((12, 10) if i < 8 else (15, 40))
            b.append(
                _b(hh, mm, buyer=20 + i, strike=strike, option_type=option_type, session=session)
            )
            # Agreement on calls, disagreement on puts.
            side = Side.SELL if option_type == "C" else Side.BUY
            c.append(
                _c(
                    end_hh,
                    end_mm,
                    20 + i,
                    side=side,
                    strike=strike,
                    option_type=option_type,
                    session=session,
                )
            )
    return b, c


def test_strata_are_the_predeclared_families_and_calls_disagree_with_puts_here():
    b, c = _two_session_fixture()
    result, cells = compare_attribution(
        b, c, config=AttributionConfig(n_boot=50, min_stratum_n=5), spot_provider=lambda ts: 6000.0
    )
    assert set(result.strata) == {
        "option_type",
        "dte",
        "moneyness",
        "time_of_day",
        "activity_size",
        "complexity",
    }
    assert (
        result.strata["option_type"]["call"]["active_cells"]["sign_agreement_both_nonzero"] == 1.0
    )
    assert result.strata["option_type"]["put"]["active_cells"]["sign_agreement_both_nonzero"] == 0.0
    assert result.strata["complexity"] == {"note": "not available in the supplied data"}
    assert set(result.strata["time_of_day"]) == {"first_30m", "midday", "final_hour"}
    assert result.n_sessions == 2
    assert result.headline["active_cells"]["sign_agreement_both_nonzero"] == pytest.approx(0.5)
    assert set(result.sensitivity) == {
        "threshold_1",
        "threshold_5",
        "threshold_10",
        "threshold_25",
        "threshold_50",
    }
    ci = result.confidence["sign_agreement_active"]
    assert ci["point"] == pytest.approx(0.5) and ci["n_sessions"] == 2


def test_report_uses_the_mandatory_terminology_and_writes_three_files(tmp_path):
    b, c = _two_session_fixture()
    result, cells = compare_attribution(b, c, config=AttributionConfig(n_boot=20))
    text = render_attribution_markdown(result, synthetic=True)
    lowered = text.lower()
    assert "aggressor-inferred" in lowered
    assert "exchange-classified" in lowered
    assert "synthetic inputs" in lowered
    assert "true dealer" not in lowered
    assert "observed dealer" not in lowered.replace("neither is observed dealer inventory", "")
    path = write_attribution_report(result, cells, tmp_path / "attribution.md", synthetic=True)
    assert path.exists()
    assert path.with_suffix(".json").exists()
    assert (tmp_path / "attribution_cells.csv").exists()
