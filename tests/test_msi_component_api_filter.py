"""Tests for the /api/signals/score component-normalization filter.

The router applies an explicit allowlist + payload reshaping to the raw
JSONB returned from ``signal_scores.components``.  Phase 2.1 changed
both the allowed component names and added passthrough for the optional
``context`` sub-dict (where ``gamma_anchor`` exposes its three
subscores).  This test pins the contract.
"""

from src.api.routers.trade_signals import _normalize_msi_components


def test_active_components_pass_through():
    raw = {
        "net_gex_sign": {"max_points": 16, "contribution": 9.6, "score": 0.6},
        "gamma_anchor": {"max_points": 30, "contribution": -2.1, "score": -0.07},
        "put_call_ratio": {"max_points": 12, "contribution": 2.4, "score": 0.2},
        "volatility_regime": {"max_points": 6, "contribution": -0.08, "score": -0.013},
        "order_flow_imbalance": {"max_points": 19, "contribution": 6.65, "score": 0.35},
        "dealer_delta_pressure": {"max_points": 17, "contribution": 3.4, "score": 0.2},
    }
    out = _normalize_msi_components(raw)
    assert set(out.keys()) == set(raw.keys())
    for name, payload in raw.items():
        assert out[name]["max_points"] == payload["max_points"]
        assert out[name]["contribution"] == round(payload["contribution"], 4)
        assert out[name]["score"] == round(payload["score"], 6)


def test_former_gamma_cluster_keys_are_filtered_out():
    """The three former gamma-cluster keys must not appear in the API
    response — they were collapsed into gamma_anchor in Phase 2.1."""
    raw = {
        "flip_distance": {"max_points": 19, "contribution": 3.0, "score": 0.16},
        "local_gamma": {"max_points": 15, "contribution": -2.0, "score": -0.13},
        "price_vs_max_gamma": {"max_points": 7, "contribution": 1.0, "score": 0.14},
        "gamma_anchor": {"max_points": 30, "contribution": 6.0, "score": 0.20},
    }
    out = _normalize_msi_components(raw)
    assert "flip_distance" not in out
    assert "local_gamma" not in out
    assert "price_vs_max_gamma" not in out
    assert "gamma_anchor" in out


def test_context_dict_passes_through():
    """gamma_anchor's nested subscores must reach the API response intact."""
    raw = {
        "gamma_anchor": {
            "max_points": 30,
            "contribution": -2.1,
            "score": -0.07,
            "context": {
                "score": -0.07,
                "flip_distance_subscore": 0.21,
                "local_gamma_subscore": -0.42,
                "price_vs_max_gamma_subscore": 0.17,
                "blend_weights": {
                    "flip_distance": 0.45,
                    "local_gamma": 0.35,
                    "price_vs_max_gamma": 0.20,
                },
            },
        },
    }
    out = _normalize_msi_components(raw)
    assert "context" in out["gamma_anchor"]
    ctx = out["gamma_anchor"]["context"]
    assert ctx["flip_distance_subscore"] == 0.21
    assert ctx["local_gamma_subscore"] == -0.42
    assert ctx["price_vs_max_gamma_subscore"] == 0.17
    assert ctx["blend_weights"]["flip_distance"] == 0.45


def test_empty_context_omitted():
    """No `context` key in the output when the source dict has none."""
    raw = {
        "gamma_anchor": {"max_points": 30, "contribution": 0.0, "score": 0.0},
    }
    out = _normalize_msi_components(raw)
    assert "context" not in out["gamma_anchor"]


def test_unknown_component_keys_are_filtered():
    """Defensive check: keys not in the allowlist never reach the response."""
    raw = {
        "gamma_anchor": {"max_points": 30, "contribution": 0.0, "score": 0.0},
        "made_up_component": {"max_points": 10, "contribution": 5.0, "score": 0.5},
    }
    out = _normalize_msi_components(raw)
    assert "made_up_component" not in out
    assert "gamma_anchor" in out


def test_non_dict_input_returned_unchanged():
    """Non-dict inputs (defensive — DB hiccup) pass through untouched."""
    assert _normalize_msi_components(None) is None
    assert _normalize_msi_components("oops") == "oops"
    assert _normalize_msi_components([1, 2]) == [1, 2]
