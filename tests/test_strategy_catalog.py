"""The strategy catalog is the single source of truth — these lock that in.

Three surfaces (TradeWorkz bots, Backtesting, Pattern Insights) read
``src/strategies``. The failure mode this consolidation existed to kill is
drift: two places describing the same thesis, disagreeing, and nobody
noticing. So the tests here are mostly *invariants over the whole catalog*
rather than examples — a new strategy added tomorrow is checked automatically.
"""

from __future__ import annotations

import pytest

from src.strategies import (
    RETIREMENT_MIN_HISTORY_DAYS,
    RETIREMENT_MIN_TRADES,
    RETIREMENT_MIN_TUNING_GENERATIONS,
    Family,
    ResearchRun,
    Retirement,
    Stage,
    StrategyEntry,
    Verdict,
    all_strategies,
    bot_bound,
    can_promote,
    can_retire,
    canonical_id,
    find,
    get,
    pattern_bound,
    provisionable,
    validate_stage,
)
from src.strategies.audit import gaps, integrity_errors
from src.strategies.catalog import _build_index
from src.tradeworkz.registry import (
    ALL_BOT_SPECS,
    CANDIDATE_SPECS,
    DEFAULT_ROSTER,
    DISABLED_BOT_IDS,
    SHELVED_SPECS,
    known_specs,
    spec_for,
)

# ---------------------------------------------------------------------------
# Integrity
# ---------------------------------------------------------------------------


def test_catalog_has_no_integrity_errors():
    """The audit's own checks, run on CI so a broken catalog fails the build.

    Covers: every bot_class resolves, every pattern_id matches a real playbook
    pattern, every playbook pattern is claimed, no bot class is orphaned or
    double-claimed, lineage points at real entries, and every stage is
    supported by its own evidence.
    """
    assert integrity_errors() == []


def test_every_id_resolves_to_exactly_one_strategy():
    index = _build_index()  # raises on a collision
    for entry in all_strategies():
        for alias in entry.aliases:
            assert index[alias] is entry


def test_legacy_ids_fold_onto_canonical_ids():
    """A bot id and a pattern id for the same thesis resolve to one strategy.

    This is what stops one strategy reading as two unrelated rows in Pattern
    Insights, and what lets the backtest harness accept either id.
    """
    assert canonical_id("gamma_flip_defender") == "gamma_flip_bounce"
    assert canonical_id("gamma_flip_bounce") == "gamma_flip_bounce"
    assert canonical_id("call_wall_rejector") == "call_wall_fade"
    assert canonical_id("vwap_reversion_scalper") == "vwap_reversion"
    assert canonical_id("not_a_strategy") is None


def test_get_raises_on_unknown_and_find_returns_none():
    with pytest.raises(KeyError):
        get("definitely_not_a_strategy")
    assert find("definitely_not_a_strategy") is None


def test_ids_are_stable_snake_case_slugs():
    for entry in all_strategies():
        assert entry.id == entry.id.lower()
        assert " " not in entry.id
        assert entry.id.replace("_", "").isalnum(), entry.id


def test_every_strategy_has_a_family_and_valid_tier():
    for entry in all_strategies():
        assert isinstance(entry.family, Family)
        assert entry.tier in ("0DTE", "1DTE", "swing"), entry.id
        assert entry.direction_mode in ("bullish", "bearish", "context", "neutral"), entry.id


# ---------------------------------------------------------------------------
# Coverage: the whole point of the consolidation
# ---------------------------------------------------------------------------


def test_every_strategy_is_backtestable():
    """Before the catalog, 12 bot-only strategies had no backtest path at all.

    If this ever fails, a strategy was added with no engine — it would be
    invisible to Backtesting and Pattern Insights.
    """
    unreachable = [e.id for e in all_strategies() if not e.backtestable]
    assert unreachable == [], f"no engine can measure: {unreachable}"


def test_bot_and_pattern_bindings_cover_both_implementations():
    assert len(bot_bound()) == 27
    assert len(pattern_bound()) == 18
    # Ten strategies carry both bindings, so the union is the catalog.
    both = [e for e in all_strategies() if e.bot_class and e.has_pattern]
    assert len(both) == 10
    assert len(bot_bound()) + len(pattern_bound()) - len(both) == len(all_strategies())


def test_registry_projects_the_catalog_without_loss():
    specs = {s.id: s for s in ALL_BOT_SPECS}
    assert len(specs) == len(bot_bound())
    for entry in bot_bound():
        spec = specs[entry.bot_id]
        assert spec.strategy_class == entry.bot_class
        assert spec.display_name == entry.name
        assert spec.tier == entry.tier
        assert spec.description == entry.thesis
        assert spec.universe == entry.universe


def test_known_specs_accepts_either_id():
    specs = known_specs()
    for entry in bot_bound():
        assert specs[entry.bot_id].id == entry.bot_id
        assert specs[entry.id].id == entry.bot_id


def test_bot_params_layer_over_catalog_params():
    """A bot tunes the general strategy to its own spec; it never forks it."""
    entry = get("vix_regime_breakout")
    # The pattern's better-evidenced floor is the catalog default...
    assert entry.params["min_vix"] == 18.0
    # ...and the bot's historical setting is an explicit, visible override.
    assert entry.bot_params["min_vix"] == 16.0
    effective = entry.effective_bot_params()
    assert effective["min_vix"] == 16.0
    # Non-overridden catalog params still come through.
    assert effective["min_move_pct"] == entry.params["min_move_pct"]


def test_a_bot_with_no_overrides_inherits_catalog_params_exactly():
    entry = get("settlement_flow_snap")
    assert entry.bot_params == {}
    assert entry.effective_bot_params() == entry.params


def test_spec_for_rejects_a_pattern_only_strategy():
    entry = get("gex_gradient_trend")
    assert entry.bot_class is None
    with pytest.raises(ValueError, match="no bot binding"):
        spec_for(entry)


# ---------------------------------------------------------------------------
# Live capital is gated on evidence, not on this module
# ---------------------------------------------------------------------------


def test_nothing_is_funded_without_validated_evidence():
    for entry in all_strategies():
        if entry.is_provisionable:
            assert entry.stage is Stage.VALIDATED
            assert entry.bot_class is not None


def test_default_roster_is_exactly_the_provisionable_set():
    assert {s.id for s in DEFAULT_ROSTER} == {e.bot_id for e in provisionable()}


def test_roster_is_empty_until_a_validated_strategy_has_a_bot():
    """Locks in today's live behavior: no paper capital on unproven strategies.

    The one validated strategy has no bot binding, so the fleet stays unfunded.
    Writing that bot is a deliberate act that will change this test.
    """
    assert DEFAULT_ROSTER == ()
    validated = [e for e in all_strategies() if e.stage is Stage.VALIDATED]
    assert validated, "expected at least one validated strategy"
    assert all(e.bot_class is None for e in validated)


def test_disabled_ids_are_only_bots_that_actually_shipped():
    """A never-provisioned strategy has no tw_bots row to turn off.

    Listing one would mean an operator who hand-inserts a candidate row to
    trial it gets it disabled out from under them on the next provision.
    """
    never_shipped = {e.bot_id for e in bot_bound() if not e.provisioned_history}
    assert never_shipped.isdisjoint(DISABLED_BOT_IDS)
    shipped = {e.bot_id for e in bot_bound() if e.provisioned_history}
    assert shipped <= set(DISABLED_BOT_IDS)
    # The two legacy symbol-specific ids keep their foreign keys valid.
    assert "qqq_gamma_flip_breaker" in DISABLED_BOT_IDS
    assert "qqq_dealer_delta_pressure_rider" in DISABLED_BOT_IDS


def test_candidates_are_runnable_but_never_disabled_or_funded():
    roster = {s.id for s in DEFAULT_ROSTER}
    for spec in CANDIDATE_SPECS:
        assert spec.id not in roster
        assert spec.id not in DISABLED_BOT_IDS
        assert spec.enabled is False


def test_shelved_specs_keeps_its_original_meaning():
    """Twelve bots shipped before the 2026-08-09 shelving; that set is stable."""
    assert len(SHELVED_SPECS) == 12
    assert {s.id for s in SHELVED_SPECS} <= set(DISABLED_BOT_IDS)


# ---------------------------------------------------------------------------
# Promotion / retirement policy
# ---------------------------------------------------------------------------


def _entry(**kw) -> StrategyEntry:
    base = dict(
        id="synthetic",
        name="Synthetic",
        family=Family.WALL,
        tier="0DTE",
        direction_mode="context",
        tagline="tagline",
        thesis="A synthetic strategy used to exercise the policy gates directly.",
        stage=Stage.RESEARCH,
    )
    base.update(kw)
    return StrategyEntry(**base)


def _run(**kw) -> ResearchRun:
    from datetime import date

    base = dict(
        ran_on=date(2026, 1, 1),
        window_days=30,
        trades=50,
        verdict=Verdict.NO_EDGE,
    )
    base.update(kw)
    return ResearchRun(**base)


def test_every_declared_stage_is_supported_by_its_evidence():
    for entry in all_strategies():
        assert validate_stage(entry) is None


def test_promotion_needs_pf_trades_and_positive_expectancy():
    assert not can_promote(_entry()).allowed  # no runs at all
    thin = _entry(research=(_run(verdict=Verdict.EDGE, profit_factor=3.0, trades=5),))
    assert not can_promote(thin).allowed
    weak = _entry(research=(_run(verdict=Verdict.EDGE, profit_factor=1.05, trades=50),))
    assert not can_promote(weak).allowed
    losing = _entry(
        research=(_run(verdict=Verdict.EDGE, profit_factor=1.5, trades=50, expectancy=-1.0),)
    )
    assert not can_promote(losing).allowed
    good = _entry(
        research=(_run(verdict=Verdict.EDGE, profit_factor=1.5, trades=50, expectancy=10.0),)
    )
    assert can_promote(good).allowed


def test_nothing_in_the_catalog_is_retirement_eligible_today():
    """The policy's practical consequence, asserted so it cannot drift.

    The deepest screen anywhere is 90 days against a five-year bar. If this
    ever fails it means either deep history landed (good) or someone recorded
    a research run they should not have.
    """
    eligible = [e.id for e in all_strategies() if can_retire(e).allowed]
    assert eligible == []
    assert max(e.deepest_window_days for e in all_strategies()) < RETIREMENT_MIN_HISTORY_DAYS


def test_retirement_requires_history_generations_and_trades():
    deep = _run(window_days=RETIREMENT_MIN_HISTORY_DAYS, trades=RETIREMENT_MIN_TRADES)
    # Deep and well-sampled, but only one tuning generation.
    one_gen = _entry(research=(deep,))
    verdict = can_retire(one_gen)
    assert not verdict.allowed
    assert any("tuning generation" in b for b in verdict.blockers)

    # Three generations, each conclusive, deep window, enough trades.
    runs = tuple(
        _run(
            window_days=RETIREMENT_MIN_HISTORY_DAYS,
            trades=RETIREMENT_MIN_TRADES,
            tuning_generation=g,
        )
        for g in range(RETIREMENT_MIN_TUNING_GENERATIONS)
    )
    assert can_retire(_entry(research=runs)).allowed


def test_shallow_history_blocks_retirement_however_many_generations():
    runs = tuple(_run(window_days=90, trades=500, tuning_generation=g) for g in range(6))
    verdict = can_retire(_entry(research=runs))
    assert not verdict.allowed
    assert any("deepest screen" in b for b in verdict.blockers)


def test_inconclusive_screens_never_count_toward_retirement():
    """An underpowered screen has not tested the thesis.

    This is the rule that keeps a promising-but-thin strategy (profit factor
    2.28 on five trades) out of the retirement pile.
    """
    runs = tuple(
        _run(
            window_days=RETIREMENT_MIN_HISTORY_DAYS,
            trades=3,
            verdict=v,
            tuning_generation=g,
        )
        for g, v in enumerate((Verdict.UNDERPOWERED, Verdict.INSUFFICIENT, Verdict.INVALID))
    )
    entry = _entry(research=runs)
    assert entry.conclusive_tuning_generations == ()
    assert not can_retire(entry).allowed


def test_a_measured_edge_blocks_retirement_outright():
    runs = tuple(
        _run(window_days=RETIREMENT_MIN_HISTORY_DAYS, trades=400, tuning_generation=g)
        for g in range(3)
    ) + (
        _run(verdict=Verdict.EDGE, profit_factor=2.0, trades=100, tuning_generation=9),
    )
    verdict = can_retire(_entry(research=runs))
    assert not verdict.allowed
    assert any("measured an edge" in b for b in verdict.blockers)


def test_retired_stage_requires_a_retirement_record_and_a_passing_gate():
    from datetime import date

    runs = tuple(
        _run(
            window_days=RETIREMENT_MIN_HISTORY_DAYS,
            trades=RETIREMENT_MIN_TRADES,
            tuning_generation=g,
        )
        for g in range(RETIREMENT_MIN_TUNING_GENERATIONS)
    )
    # Retired with the evidence but no audit record.
    assert "without a Retirement record" in (
        validate_stage(_entry(stage=Stage.RETIRED, research=runs)) or ""
    )
    # Retired without the evidence.
    shallow = _entry(
        stage=Stage.RETIRED,
        research=(_run(),),
        retirement=Retirement(
            decided_on=date(2026, 1, 1),
            history_days=30,
            tuning_generations=1,
            rationale="premature",
        ),
    )
    assert "stage=retired but" in (validate_stage(shallow) or "")
    # Fully supported.
    ok = _entry(
        stage=Stage.RETIRED,
        research=runs,
        retirement=Retirement(
            decided_on=date(2026, 1, 1),
            history_days=RETIREMENT_MIN_HISTORY_DAYS,
            tuning_generations=RETIREMENT_MIN_TUNING_GENERATIONS,
            rationale="exhausted",
        ),
    )
    assert validate_stage(ok) is None


def test_superseded_strategies_name_their_successor():
    for entry in all_strategies():
        if entry.stage is Stage.SUPERSEDED:
            assert entry.superseded_by, entry.id
            assert find(entry.superseded_by) is not None, entry.id


# ---------------------------------------------------------------------------
# Audit surface
# ---------------------------------------------------------------------------


def test_audit_reports_the_validated_without_bot_gap():
    """The highest-value gap: proven edge, nothing able to trade it."""
    assert gaps()["validated_without_bot"] == ["gex_gradient_trend"]
    assert gaps()["retirement_eligible"] == []
    assert gaps()["no_engine"] == []


def test_audit_renders_without_error():
    from src.strategies.audit import render

    out = render(list(all_strategies()))
    assert "ZeroGEX strategy catalog" in out
    assert "NOTHING is retirement-eligible" in out
    for entry in all_strategies():
        assert entry.id in out
