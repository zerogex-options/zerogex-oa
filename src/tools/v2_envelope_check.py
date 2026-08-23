"""Verify the /api/v2 surface returns a well-formed freshness envelope.

``make api-test V=2`` proves every endpoint answers 2xx. It cannot prove the
BODY is right — and a 200 carrying the wrong shape is the v2 failure mode
that matters, because the whole point of the version is the envelope. A
mirror that silently stopped wrapping, or an upstream middleware that
rewrote the body (the futures projection nearly did exactly that), returns
a perfectly good 200 the whole way.

So this checks the contract itself: exactly ``{"data": ..., "freshness":
{...}}`` at the top level, and every declared freshness field present. The
"every field" part is not pedantry — v2 promises a consumer can index
``freshness.source_timestamp`` unconditionally rather than testing for the
key, and that promise only holds if no field is ever dropped.

Deliberately stdlib-only (urllib, no requests/httpx) so it runs from a
systemd ExecStartPost or a bare deploy shell with nothing installed.

Exit codes:
    0 — every endpoint returned a complete envelope.
    1 — at least one did not.
    2 — could not reach the server at all.

Usage:
    python -m src.tools.v2_envelope_check
    python -m src.tools.v2_envelope_check --version 2 --symbol SPY
    python -m src.tools.v2_envelope_check --path /api/v2/gex/summary?symbol=SPY
    API_KEY=... python -m src.tools.v2_envelope_check --json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from typing import Any, Sequence

# The envelope's declared fields, spelled out rather than imported.
#
# Importing src.api.freshness would drag in src/api/__init__.py, which builds
# the entire FastAPI app — seconds of startup and a wall of INFO logging in
# the middle of a check whose whole output is five lines. It also means this
# runs from a deploy shell or a systemd ExecStartPost with nothing on the
# path but the stdlib.
#
# The copy cannot drift: tests/test_v2_envelope_check.py asserts this set
# equals Freshness.model_fields, so adding a field to the model without
# adding it here fails CI.
_ENVELOPE_FIELDS = frozenset(
    {
        "evaluated_at",
        "generated_at",
        "source_timestamp",
        "latest_event_at",
        "age_seconds",
        "market_session_status",
        "expected_update_cadence",
        "expected_update_cadence_seconds",
        "cadence_profile",
        "stale_after",
        "freshness_status",
    }
)


def envelope_fields() -> frozenset[str]:
    """The fields a v2 ``freshness`` block must carry."""
    return _ENVELOPE_FIELDS


@dataclass
class EndpointResult:
    path: str
    status: int | None
    ok: bool
    detail: str


def check_body(body: Any, *, fields: frozenset[str]) -> tuple[bool, str]:
    """Grade one parsed response body. Pure — no I/O, so it is testable."""
    if not isinstance(body, dict):
        return False, f"not an object (got {type(body).__name__})"
    keys = set(body)
    if keys != {"data", "freshness"}:
        extra = sorted(keys - {"data", "freshness"})
        missing = sorted({"data", "freshness"} - keys)
        bits = []
        if missing:
            bits.append("missing " + ",".join(missing))
        if extra:
            bits.append("unexpected " + ",".join(extra))
        return False, "not enveloped (" + "; ".join(bits) + ")"
    freshness = body["freshness"]
    if not isinstance(freshness, dict):
        return False, f"freshness is {type(freshness).__name__}, not an object"
    absent = sorted(fields - set(freshness))
    if absent:
        return False, "freshness missing " + ",".join(absent)
    return True, f"{freshness.get('freshness_status')} / {freshness.get('cadence_profile')}"


def default_paths(version: str, symbol: str) -> list[str]:
    """A spread across cadence profiles and response shapes.

    One from each of: on-demand, the analytics snapshot, the consolidated
    contract with a path parameter, a JSONResponse-returning route, and a
    route with no response_model (the class where the encoder divergence
    lived).
    """
    v = f"/api/v{version}"
    return [
        f"{v}/health",
        f"{v}/gex/summary?symbol={symbol}",
        f"{v}/levels/{symbol}",
        f"{v}/flow/series?symbol={symbol}",
        f"{v}/signals/score?underlying={symbol}",
    ]


def fetch(url: str, api_key: str | None, timeout: float) -> tuple[int | None, Any, str]:
    req = urllib.request.Request(url)
    if api_key:
        req.add_header("X-API-Key", api_key)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            status = resp.status
    except urllib.error.HTTPError as e:
        return e.code, None, f"HTTP {e.code}"
    except Exception as e:  # noqa: BLE001 - connection refused, DNS, timeout
        return None, None, f"unreachable ({e.__class__.__name__})"
    try:
        return status, json.loads(raw), ""
    except ValueError:
        return status, None, "response was not JSON"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--version", default="2", help="API version to probe (default: 2).")
    parser.add_argument("--symbol", default="SPY")
    parser.add_argument(
        "--path",
        action="append",
        default=None,
        help="Probe this exact path instead of the defaults. Repeatable.",
    )
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    args = parser.parse_args(argv)

    # Read the key from the environment rather than a flag so it never lands
    # in a process listing.
    api_key = os.getenv("OPS_API_KEY") or os.getenv("API_KEY") or None
    paths = args.path or default_paths(args.version, args.symbol)
    fields = envelope_fields()

    results: list[EndpointResult] = []
    unreachable = 0
    for path in paths:
        status, body, err = fetch(args.base_url + path, api_key, args.timeout)
        if status is None:
            unreachable += 1
            results.append(EndpointResult(path, None, False, err))
            continue
        if err:
            results.append(EndpointResult(path, status, False, err))
            continue
        ok, detail = check_body(body, fields=fields)
        results.append(EndpointResult(path, status, ok, detail))

    if args.json:
        print(json.dumps([asdict(r) for r in results], indent=2))
    else:
        for r in results:
            mark = "OK  " if r.ok else "FAIL"
            print(f"  {mark} {r.path:52s} {r.detail}")

    if unreachable == len(results) and results:
        print(f"could not reach {args.base_url} — is the API running?", file=sys.stderr)
        return 2
    bad = [r for r in results if not r.ok]
    if bad:
        print(
            f"{len(bad)} of {len(results)} endpoint(s) did not return a complete "
            f"freshness envelope",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
