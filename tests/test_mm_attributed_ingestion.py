"""Cboe ingestion: normalization, profile safety, and schema discovery.

No Cboe sample existed when this was built, so the ingestion layer's contract
is: *never guess, and never let a guess become a result*.  These tests pin that
contract — an unconfirmed profile refuses to run, a profile whose columns are
absent fails loudly, and the inspector proposes a mapping from a real header
while naming everything it could not map.
"""

from __future__ import annotations

import csv
import gzip
import json
import zipfile
from dataclasses import replace
from datetime import date, datetime, time, timezone
from pathlib import Path

import pytest

from research.mm_attributed_gex.cboe.inspect import propose_profile, render_proposal
from research.mm_attributed_gex.cboe.loader import (
    LoadResult,
    ProfileMismatch,
    ProfileNotConfirmed,
    load_file,
    load_paths,
    records_from_rows,
)
from research.mm_attributed_gex.cboe.profiles import (
    ColumnProfile,
    VolumeColumn,
    builtin_profiles,
    get_profile,
    load_profile_file,
    save_profile_file,
)
from research.mm_attributed_gex.schema import (
    Exchange,
    Interval,
    ParticipantActivity,
    ParticipantType,
    PositionEffect,
    Side,
    normalize_option_type,
)

WIDE_HEADER = [
    "underlying_symbol",
    "option_root",
    "trade_date",
    "interval_end",
    "expiration",
    "strike",
    "call_put",
    "market_maker_open_buy",
    "market_maker_open_sell",
    "market_maker_close_buy",
    "market_maker_close_sell",
    "customer_open_buy",
    "customer_open_sell",
    "firm_open_buy",
]


def _wide_row(**overrides):
    row = {
        "underlying_symbol": "SPX",
        "option_root": "SPXW",
        "trade_date": "2026-06-15",
        "interval_end": "10:00:00",
        "expiration": "2026-06-19",
        "strike": "6000",
        "call_put": "C",
        "market_maker_open_buy": "100",
        "market_maker_open_sell": "0",
        "market_maker_close_buy": "0",
        "market_maker_close_sell": "40",
        "customer_open_buy": "0",
        "customer_open_sell": "100",
        "firm_open_buy": "0",
    }
    row.update({k: str(v) for k, v in overrides.items()})
    return row


def _confirmed_wide_profile() -> ColumnProfile:
    return ColumnProfile(
        name="test_wide",
        layout="wide",
        exchange=Exchange.CBOE_C1,
        interval=Interval.MINUTE_30,
        confirmed=True,
        symbol_col="underlying_symbol",
        option_root_col="option_root",
        expiration_col="expiration",
        strike_col="strike",
        option_type_col="call_put",
        trading_date_col="trade_date",
        interval_end_col="interval_end",
        volume_columns=(
            VolumeColumn(
                "market_maker_open_buy",
                ParticipantType.MARKET_MAKER,
                Side.BUY,
                PositionEffect.OPEN,
            ),
            VolumeColumn(
                "market_maker_open_sell",
                ParticipantType.MARKET_MAKER,
                Side.SELL,
                PositionEffect.OPEN,
            ),
            VolumeColumn(
                "market_maker_close_buy",
                ParticipantType.MARKET_MAKER,
                Side.BUY,
                PositionEffect.CLOSE,
            ),
            VolumeColumn(
                "market_maker_close_sell",
                ParticipantType.MARKET_MAKER,
                Side.SELL,
                PositionEffect.CLOSE,
            ),
            VolumeColumn(
                "customer_open_buy", ParticipantType.CUSTOMER, Side.BUY, PositionEffect.OPEN
            ),
            VolumeColumn(
                "customer_open_sell", ParticipantType.CUSTOMER, Side.SELL, PositionEffect.OPEN
            ),
            VolumeColumn("firm_open_buy", ParticipantType.FIRM, Side.BUY, PositionEffect.OPEN),
        ),
        default_symbol="SPX",
    )


def _write_csv(path: Path, rows, header=WIDE_HEADER) -> Path:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
    return path


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------


def test_option_type_accepts_real_spellings_and_rejects_anything_else():
    for raw in ("C", "c", "Call", "CALLS"):
        assert normalize_option_type(raw) == "C"
    for raw in ("P", "put", "Puts"):
        assert normalize_option_type(raw) == "P"
    with pytest.raises(ValueError):
        normalize_option_type("X")
    with pytest.raises(ValueError):
        normalize_option_type(None)


def test_activity_records_reject_negative_volume():
    """Open-Close volumes are counts; direction lives in side/effect."""
    with pytest.raises(ValueError, match="non-negative"):
        ParticipantActivity(
            exchange=Exchange.CBOE_C1,
            symbol="SPX",
            expiration=date(2026, 6, 19),
            strike=6000.0,
            option_type="C",
            participant=ParticipantType.MARKET_MAKER,
            side=Side.BUY,
            position_effect=PositionEffect.OPEN,
            contracts=-5,
            timestamp=datetime(2026, 6, 15, 14, tzinfo=timezone.utc),
            trading_date=date(2026, 6, 15),
        )


def test_wide_row_expands_to_one_record_per_non_zero_bucket():
    result = LoadResult()
    records = list(records_from_rows([_wide_row()], _confirmed_wide_profile(), result=result))
    assert {(r.participant, r.side, r.position_effect, r.contracts) for r in records} == {
        (ParticipantType.MARKET_MAKER, Side.BUY, PositionEffect.OPEN, 100),
        (ParticipantType.MARKET_MAKER, Side.SELL, PositionEffect.CLOSE, 40),
        (ParticipantType.CUSTOMER, Side.SELL, PositionEffect.OPEN, 100),
    }
    # Zero buckets carry no information for a running sum, so they are counted
    # and dropped rather than materialized.
    assert result.zero_volume_buckets == 4
    assert result.mm_contracts_total == 140


def test_timestamps_land_in_utc_from_an_eastern_interval_end():
    records = list(records_from_rows([_wide_row()], _confirmed_wide_profile()))
    ts = records[0].timestamp
    assert ts.tzinfo is not None
    assert ts.utcoffset().total_seconds() == 0
    assert ts.hour == 14  # 10:00 ET in June is 14:00 UTC


def test_session_summary_rows_are_stamped_at_the_session_close():
    """No interval column means an end-of-day file: stamp it at 16:00 ET.

    Stamping at the close is what keeps the replay causal — a session summary
    must not inform a snapshot taken inside that same session.
    """
    profile = replace(_confirmed_wide_profile(), interval=Interval.SESSION, interval_end_col=None)
    records = list(records_from_rows([_wide_row()], profile))
    from zoneinfo import ZoneInfo

    assert records[0].timestamp.astimezone(ZoneInfo("America/New_York")).time() == time(16, 0)


def test_decorated_symbols_canonicalize_and_keep_the_listed_root():
    records = list(
        records_from_rows(
            [_wide_row(underlying_symbol="$SPX.X", option_root="SPXW")],
            _confirmed_wide_profile(),
        )
    )
    assert records[0].symbol == "SPX"
    assert records[0].option_root == "SPXW"


def test_strike_scale_converts_a_thousandths_feed():
    scaled = replace(_confirmed_wide_profile(), strike_scale=0.001)
    records = list(records_from_rows([_wide_row(strike="6000000")], scaled))
    assert records[0].strike == pytest.approx(6000.0)


def test_unparseable_rows_are_skipped_and_reported_not_silently_dropped():
    result = LoadResult()
    records = list(
        records_from_rows(
            [_wide_row(), _wide_row(expiration="not-a-date")],
            _confirmed_wide_profile(),
            result=result,
        )
    )
    assert len(records) == 3  # only the good row expanded
    assert result.rows_skipped == 1
    assert result.errors and "unparseable date" in result.errors[0]


def test_negative_volume_cells_are_rejected_with_an_explanation():
    result = LoadResult()
    list(
        records_from_rows(
            [_wide_row(market_maker_open_buy="-10")],
            _confirmed_wide_profile(),
            result=result,
        )
    )
    assert any("negative volume" in e for e in result.errors)


def test_long_layout_maps_cell_values_to_enums():
    profile = ColumnProfile(
        name="long",
        layout="long",
        confirmed=True,
        symbol_col="underlying_symbol",
        expiration_col="expiration",
        strike_col="strike",
        option_type_col="option_type",
        trading_date_col="trade_date",
        interval_end_col="interval_end",
        participant_col="participant",
        side_col="side",
        effect_col="position_effect",
        contracts_col="contracts",
        default_symbol="SPX",
    )
    rows = [
        {
            "underlying_symbol": "SPX",
            "trade_date": "2026-06-15",
            "interval_end": "10:00",
            "expiration": "2026-06-19",
            "strike": "6000",
            "option_type": "PUT",
            "participant": "Market Maker",
            "side": "Sell",
            "position_effect": "Open",
            "contracts": "250",
        }
    ]
    records = list(records_from_rows(rows, profile))
    assert len(records) == 1
    rec = records[0]
    assert rec.participant is ParticipantType.MARKET_MAKER
    assert rec.side is Side.SELL
    assert rec.position_effect is PositionEffect.OPEN
    assert rec.option_type == "P"
    assert rec.contracts == 250


def test_professional_customer_is_not_swallowed_by_customer():
    profile = ColumnProfile(
        name="long",
        layout="long",
        confirmed=True,
        expiration_col="expiration",
        strike_col="strike",
        option_type_col="option_type",
        trading_date_col="trade_date",
        participant_col="participant",
        side_col="side",
        effect_col="position_effect",
        contracts_col="contracts",
        default_symbol="SPX",
    )
    rows = [
        {
            "trade_date": "2026-06-15",
            "expiration": "2026-06-19",
            "strike": "6000",
            "option_type": "C",
            "participant": "professional_customer",
            "side": "buy",
            "position_effect": "open",
            "contracts": "10",
        }
    ]
    assert (
        list(records_from_rows(rows, profile))[0].participant
        is ParticipantType.PROFESSIONAL_CUSTOMER
    )


# ---------------------------------------------------------------------------
# Profile safety
# ---------------------------------------------------------------------------


def test_unconfirmed_profile_refuses_to_load(tmp_path):
    """A guessed mapping must never quietly become a research result."""
    path = _write_csv(tmp_path / "oc.csv", [_wide_row()])
    unconfirmed = get_profile("cboe_open_close_v1")
    assert unconfirmed.confirmed is False
    with pytest.raises(ProfileNotConfirmed, match="not confirmed"):
        list(load_file(path, unconfirmed))


def test_profile_whose_columns_are_absent_fails_loudly(tmp_path):
    path = _write_csv(tmp_path / "oc.csv", [_wide_row()])
    bad = get_profile("cboe_open_close_v1").with_confirmed(True)
    with pytest.raises(ProfileMismatch, match="not found in file header"):
        list(load_file(path, bad))


def test_validate_requires_a_market_maker_column():
    profile = ColumnProfile(
        name="no_mm",
        layout="wide",
        confirmed=True,
        symbol_col="underlying_symbol",
        expiration_col="expiration",
        strike_col="strike",
        option_type_col="call_put",
        trading_date_col="trade_date",
        volume_columns=(
            VolumeColumn(
                "customer_open_buy", ParticipantType.CUSTOMER, Side.BUY, PositionEffect.OPEN
            ),
        ),
    )
    problems = profile.validate(WIDE_HEADER)
    assert any("MARKET_MAKER" in p for p in problems)


def test_builtin_template_is_explicitly_unconfirmed():
    """The shipped Cboe template is a starting point, not an assertion."""
    profile = builtin_profiles()["cboe_open_close_v1"]
    assert profile.confirmed is False
    assert "unconfirmed" in profile.notes.lower()
    assert len(profile.market_maker_columns()) == 4


def test_profile_round_trips_through_json(tmp_path):
    original = _confirmed_wide_profile()
    path = save_profile_file(original, tmp_path / "profile.json")
    restored = load_profile_file(path)
    assert restored.volume_columns == original.volume_columns
    assert restored.confirmed is True
    assert restored.layout == original.layout
    assert json.loads(path.read_text())["name"] == "test_wide"


def test_unknown_builtin_profile_name_raises_with_the_available_names():
    with pytest.raises(KeyError, match="cboe_open_close_v1"):
        get_profile("nope")


# ---------------------------------------------------------------------------
# Containers
# ---------------------------------------------------------------------------


def test_loads_plain_csv_gzip_and_zip(tmp_path):
    rows = [_wide_row()]
    plain = _write_csv(tmp_path / "a.csv", rows)
    gz = tmp_path / "b.csv.gz"
    with gzip.open(gz, "wt", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=WIDE_HEADER)
        writer.writeheader()
        writer.writerows(rows)
    zp = tmp_path / "c.zip"
    with zipfile.ZipFile(zp, "w") as zf:
        zf.write(plain, arcname="inner.csv")

    profile = _confirmed_wide_profile()
    for path in (plain, gz, zp):
        loaded = list(load_file(path, profile))
        assert len(loaded) == 3, path


def test_load_paths_walks_a_directory_in_sorted_order(tmp_path):
    """Chronological order is required — the inventory recursion is a running sum."""
    _write_csv(tmp_path / "20260615.csv", [_wide_row(trade_date="2026-06-15")])
    _write_csv(tmp_path / "20260616.csv", [_wide_row(trade_date="2026-06-16")])
    result = LoadResult()
    records = list(load_paths([tmp_path], _confirmed_wide_profile(), result=result))
    dates = [r.trading_date for r in records]
    assert dates == sorted(dates)
    assert result.files == 2
    assert result.trading_dates == {date(2026, 6, 15), date(2026, 6, 16)}


# ---------------------------------------------------------------------------
# Schema discovery
# ---------------------------------------------------------------------------


def test_inspector_proposes_a_usable_mapping_from_a_real_header(tmp_path):
    path = _write_csv(tmp_path / "sample.csv", [_wide_row()])
    proposal = propose_profile(path)
    assert proposal.usable
    assert proposal.profile is not None
    assert proposal.profile.confirmed is False  # never self-certifies
    assert proposal.field_map["strike"] == "strike"
    assert proposal.field_map["option_type"] == "call_put"
    mm = [vc for vc in proposal.volume_map if vc.participant is ParticipantType.MARKET_MAKER]
    assert len(mm) == 4
    assert {(vc.side, vc.position_effect) for vc in mm} == {
        (Side.BUY, PositionEffect.OPEN),
        (Side.SELL, PositionEffect.OPEN),
        (Side.BUY, PositionEffect.CLOSE),
        (Side.SELL, PositionEffect.CLOSE),
    }
    assert "PROPOSAL" in render_proposal(proposal)


def test_inspector_blocks_when_no_market_maker_column_is_present(tmp_path):
    header = [c for c in WIDE_HEADER if not c.startswith("market_maker")]
    row = {k: v for k, v in _wide_row().items() if k in header}
    path = _write_csv(tmp_path / "no_mm.csv", [row], header=header)
    proposal = propose_profile(path)
    assert not proposal.usable
    assert any("Market Maker" in p for p in proposal.blocking_problems)


def test_inspector_reports_unmapped_columns_rather_than_ignoring_them(tmp_path):
    header = WIDE_HEADER + ["some_vendor_extra"]
    row = {**_wide_row(), "some_vendor_extra": "x"}
    path = _write_csv(tmp_path / "extra.csv", [row], header=header)
    proposal = propose_profile(path)
    assert "some_vendor_extra" in proposal.unmapped
    assert any("not mapped" in w for w in proposal.warnings)


def test_inspector_flags_a_scaled_strike_column(tmp_path):
    path = _write_csv(tmp_path / "scaled.csv", [_wide_row(strike="6000000")])
    proposal = propose_profile(path)
    assert any("strike_scale" in w for w in proposal.warnings)


def test_inspector_proposal_can_be_saved_and_reloaded(tmp_path):
    path = _write_csv(tmp_path / "sample.csv", [_wide_row()])
    proposal = propose_profile(path)
    saved = save_profile_file(proposal.profile, tmp_path / "proposed.json")
    reloaded = load_profile_file(saved)
    assert reloaded.confirmed is False
    with pytest.raises(ProfileNotConfirmed):
        list(load_file(path, reloaded))
    # Once a human confirms it, the same mapping loads the file cleanly.
    confirmed = reloaded.with_confirmed(True)
    assert len(list(load_file(path, confirmed))) == 3


# ---------------------------------------------------------------------------
# Regressions found by walking the workflow by hand
# ---------------------------------------------------------------------------


def test_missing_profile_file_says_the_file_is_missing(tmp_path):
    """A path that doesn't exist must not report as an unknown built-in NAME.

    The fallback to the built-in lookup turned "you haven't created
    profile.json yet" into "Unknown profile 'research_output/cboe_profile.json';
    built-ins are [...]", which sends the reader hunting for a name that was
    never the problem.
    """
    missing = tmp_path / "not_created_yet.json"
    with pytest.raises(FileNotFoundError) as exc:
        load_profile_file(missing)
    message = str(exc.value)
    assert "No profile file at" in message
    assert "inspect-cboe" in message  # tells you how to create one
    assert "built-ins are" not in message


def test_a_bare_name_still_resolves_to_a_builtin():
    """The built-in lookup still works for values that aren't path-shaped."""
    assert load_profile_file("cboe_open_close_v1").name == "cboe_open_close_v1"


def test_directory_walk_skips_readmes_and_still_loads_the_data(tmp_path):
    """Real deliveries ship a readme next to the data; it is not a parse failure."""
    _write_csv(tmp_path / "oc_20260615.csv", [_wide_row()])
    (tmp_path / "README.txt").write_text("delivery notes", encoding="utf-8")
    (tmp_path / "MANIFEST.txt").write_text("checksums", encoding="utf-8")

    result = LoadResult()
    records = list(load_paths([tmp_path], _confirmed_wide_profile(), result=result))
    assert len(records) == 3
    assert result.files == 1
    assert result.files_skipped == 0  # documentation is filtered, not "skipped"


def test_directory_walk_skips_a_non_matching_member_but_keeps_going(tmp_path):
    """One stray CSV with a different header must not abort the whole run."""
    _write_csv(tmp_path / "oc_20260615.csv", [_wide_row()])
    with (tmp_path / "something_else.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["a", "b"])
        writer.writeheader()
        writer.writerow({"a": 1, "b": 2})

    result = LoadResult()
    records = list(load_paths([tmp_path], _confirmed_wide_profile(), result=result))
    assert len(records) == 3
    assert result.files == 1
    assert result.files_skipped == 1
    assert any("header mismatch" in e for e in result.errors)


def test_directory_where_nothing_matches_raises_rather_than_yielding_nothing(tmp_path):
    """Zero records must not look like a quiet day with no trading."""
    with (tmp_path / "wrong.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["a", "b"])
        writer.writeheader()
        writer.writerow({"a": 1, "b": 2})
    with pytest.raises(ProfileMismatch, match="none of the"):
        list(load_paths([tmp_path], _confirmed_wide_profile()))


def test_an_explicitly_named_file_still_fails_hard_on_mismatch(tmp_path):
    """Naming a file asserts it is data, so a mismatch there is an error."""
    with (tmp_path / "wrong.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["a", "b"])
        writer.writeheader()
        writer.writerow({"a": 1, "b": 2})
    with pytest.raises(ProfileMismatch):
        list(load_paths([tmp_path / "wrong.csv"], _confirmed_wide_profile()))


def test_confirming_a_profile_requires_an_explicit_review_flag(tmp_path, capsys):
    """The gate must be a deliberate act, but not a hand-edit.

    Leaving confirmation as "now edit this JSON by hand" made it the one step
    in the sequence that silently does not happen — the operator then hits a
    refusal they read as a bug. As a command it cannot be silently skipped,
    and ``--reviewed`` keeps the act deliberate.
    """
    from research.mm_attributed_gex.cli import main

    proposal = save_profile_file(
        _confirmed_wide_profile().with_confirmed(False), tmp_path / "profile.json"
    )

    # Without the flag: prints the mapping, refuses, leaves the file alone.
    assert main(["confirm-profile", str(proposal)]) == 2
    captured = capsys.readouterr()
    assert "market_maker_open_buy" in captured.out  # you see what you'd confirm
    assert "NOT CONFIRMED" in captured.err
    assert load_profile_file(proposal).confirmed is False

    # With the flag: confirmed and usable.
    assert main(["confirm-profile", str(proposal), "--reviewed"]) == 0
    assert load_profile_file(proposal).confirmed is True


def test_confirming_an_already_confirmed_profile_is_a_no_op(tmp_path):
    from research.mm_attributed_gex.cli import main

    path = save_profile_file(_confirmed_wide_profile(), tmp_path / "profile.json")
    assert main(["confirm-profile", str(path)]) == 0
    assert load_profile_file(path).confirmed is True
