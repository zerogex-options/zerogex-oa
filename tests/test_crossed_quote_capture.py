"""The capture that backs a vendor bug report has to be right.

ThetaData is fixing their Market Value calculation off the rows this
produces. A row that is wrong -- a locked quote reported as crossed, a
cross width that is off by one, a vendor timestamp silently replaced by
ours -- sends their team after the wrong thing, which is worse than
sending nothing.
"""

from __future__ import annotations

import io
from datetime import date, datetime, timezone
from pathlib import Path

from src.ingestion.providers.base import OptionQuote
from src.tools.crossed_quote_capture import CSV_COLUMNS, _summarise, crossed_rows
from src.tools.feed_compare import FeedSample

CAPTURED = datetime(2026, 9, 29, 13, 45, 0, tzinfo=timezone.utc)
QUOTED = datetime(2026, 9, 29, 13, 44, 58, tzinfo=timezone.utc)
EXPIRATION = date(2026, 9, 29)

TOOL_SRC = Path(__file__).resolve().parents[1] / "src" / "tools" / "crossed_quote_capture.py"


def _sample(quotes: dict, spot: float = 765.0) -> FeedSample:
    metadata = {
        symbol: {
            "strike": 763.0 + i,
            "expiration": EXPIRATION,
            "option_type": "C" if i % 2 == 0 else "P",
        }
        for i, symbol in enumerate(sorted(quotes))
    }
    return FeedSample(
        provider="thetadata_mv",
        captured_at=CAPTURED,
        spot=spot,
        quotes=quotes,
        metadata=metadata,
    )


def _quote(symbol: str, bid, ask, timestamp=QUOTED) -> OptionQuote:
    return OptionQuote(option_symbol=symbol, timestamp=timestamp, bid=bid, ask=ask)


def test_only_genuinely_crossed_contracts_are_reported():
    """Locked is not crossed.

    bid == ask is a legitimate state for a tight ATM contract -- the
    classifier's own fallback gate was tightened to strict ``ask < bid`` for
    exactly this reason. Reporting locked quotes to the vendor as crossed
    would pad the finding with rows that are not the bug.
    """
    rows = crossed_rows(
        _sample(
            {
                "CROSSED": _quote("CROSSED", bid=1.36, ask=1.35),
                "LOCKED": _quote("LOCKED", bid=1.35, ask=1.35),
                "NORMAL": _quote("NORMAL", bid=1.35, ask=1.36),
            }
        )
    )
    assert [r["symbol"] for r in rows] == ["CROSSED"]


def test_a_one_cent_cross_reports_as_one_cent():
    """The claim being checked is 'crossed by exactly one cent'.

    1.36 - 1.35 in floating point is 0.010000000000000675. Reporting that
    raw would make every row look like a different width and bury the
    pattern their team is looking for.
    """
    rows = crossed_rows(_sample({"X": _quote("X", bid=1.36, ask=1.35)}))
    assert rows[0]["cross_cents"] == 1


def test_a_wider_cross_is_not_rounded_down_to_one():
    """If the bug ever widens, the capture has to show it."""
    rows = crossed_rows(_sample({"X": _quote("X", bid=1.40, ask=1.35)}))
    assert rows[0]["cross_cents"] == 5


def test_the_vendor_timestamp_is_carried_not_our_own():
    """Their team lines these rows up against their raw NBBO by timestamp.

    Substituting our capture time would put every row a second or two off
    the quote it describes, and the comparison they run would miss.
    """
    rows = crossed_rows(_sample({"X": _quote("X", bid=1.36, ask=1.35)}))
    assert rows[0]["quote_timestamp"] == QUOTED.isoformat()
    assert rows[0]["captured_at_utc"] == CAPTURED.isoformat()
    assert rows[0]["quote_timestamp"] != rows[0]["captured_at_utc"]


def test_a_one_sided_quote_is_not_a_cross():
    """A missing side is absent data, not a crossed book."""
    rows = crossed_rows(
        _sample(
            {
                "NO_ASK": _quote("NO_ASK", bid=1.36, ask=None),
                "NO_BID": _quote("NO_BID", bid=None, ask=1.35),
            }
        )
    )
    assert rows == []


def test_rows_carry_the_contract_so_the_csv_stands_alone():
    """Strike, expiration and right, so no covering email is needed."""
    rows = crossed_rows(_sample({"X": _quote("X", bid=1.36, ask=1.35)}))
    row = rows[0]
    assert row["expiration"] == EXPIRATION
    assert row["strike"] == 763.0
    assert row["right"] == "C"
    assert row["underlying_spot"] == 765.0
    assert set(row) == set(CSV_COLUMNS), "a column would be dropped or left blank"


def test_a_missing_timestamp_does_not_crash_the_capture():
    """A row without one is still worth reporting; it just carries no time."""
    rows = crossed_rows(_sample({"X": _quote("X", bid=1.36, ask=1.35, timestamp=None)}))
    assert rows[0]["quote_timestamp"] == ""


def test_an_empty_capture_says_so_rather_than_implying_a_finding():
    """Sending 'no crossed quotes' as if it were evidence would mislead."""
    out = _summarise([], samples=30, quoted=7200)
    assert "NO crossed quotes" in out
    assert "Nothing to send" in out


def test_the_summary_reports_the_cross_width_distribution():
    """The pattern is the finding, not the count."""
    rows = crossed_rows(
        _sample(
            {
                "A": _quote("A", bid=1.36, ask=1.35),
                "B": _quote("B", bid=2.41, ask=2.40),
                "C": _quote("C", bid=1.40, ask=1.35),
            }
        )
    )
    out = _summarise(rows, samples=1, quoted=3)
    assert "1c x2" in out
    assert "5c x1" in out
    assert "3 distinct contracts" in out


def test_the_capture_never_reads_the_realtime_quote_endpoint():
    """Whether we may read raw NBBO at all is an open licensing question.

    Our exchange-fee exemption is scoped to the Market Value product;
    option_snapshot_quote returns raw OPRA NBBO and is not in Exhibit A.
    ThetaData suggested it as a workaround, which may yet prove fine -- but
    a diagnostic must not be what silently settles it.
    """
    source = io.open(TOOL_SRC, encoding="utf-8").read()
    body = source[source.index("def crossed_rows") :]
    assert "option_snapshot_quote" not in body
    assert "snapshot_quote" not in body
