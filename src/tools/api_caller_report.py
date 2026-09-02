"""Attribute API traffic to the key (and owner) that authenticated it.

Answers "who is this IP / what is this key doing", which no single log can
answer on its own:

  * ``/var/log/nginx/access.log`` has the client IP and the User-Agent but
    deliberately carries **no credential** — the ``zerogex_scrubbed``
    log_format (``deploy/steps/120.nginx_api``) logs only
    ``$remote_addr $request $status $http_referer $http_user_agent``, and a
    ``map`` rewrites any ``?api_key=`` to ``REDACTED`` before it is written.
  * The API's own audit line (``src.api.middleware.AuditLogMiddleware``,
    emitted on the ``src.api.audit`` logger into the journal) carries the
    resolved identity — ``caller_user_id``, ``caller_key_id``,
    ``caller_name`` — and, since the ``client_ip`` field was added, the
    client address too.

So this tool reads both and reports one row per caller, enriched from the
``api_keys`` table (prefix, scopes, created/last-used/revoked) when the DB
is reachable.

TWO MODES, PICKED AUTOMATICALLY
    direct  — audit lines carry ``client_ip=``. Attribution is an exact
              per-request fact.
    legacy  — audit lines predate that field (an API that has not been
              restarted since the change). Falls back to deciding who owns
              each ADDRESS from aggregate evidence: requests that pair
              unambiguously on (second, method, path, status) each vote for
              (caller, address), and an address is awarded only on a
              decisive majority with real support behind it. Everything else
              is DISCARDED rather than guessed at, and the report says how
              many were dropped. Absence of evidence for an IP in legacy
              mode means "could not attribute", never "not authenticated".

              Expect most requests to drop on a busy window: popular paths
              collide constantly and cannot vote. It is also a heuristic,
              not a proof — it never sees a request that skipped nginx. A
              restart, which puts client_ip on the line, is the real fix.

READ-ONLY. Runs SELECTs against ``api_keys`` and reads logs; changes nothing.

Usage:
    python -m src.tools.api_caller_report                     # everyone, last 2h
    python -m src.tools.api_caller_report --hours 12
    python -m src.tools.api_caller_report --ip 23.115.8.132   # who is this IP
    python -m src.tools.api_caller_report --user alice@example.com
    python -m src.tools.api_caller_report --key-id 7
    python -m src.tools.api_caller_report --ua-contains Gexa   # by client build
    python -m src.tools.api_caller_report --json /tmp/callers.json

Or via make (see ``make api-caller-report``).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------

# One audit line, e.g.
#   api_request method=GET path=/api/gex/summary status=200 \
#   client_ip=23.115.8.132 caller_kind=db caller_user_id=alice@example.com \
#   caller_key_id=7 caller_name=alice-laptop end_user_id=- duration_ms=3.1
# Every value is a single whitespace-free token by construction
# (``middleware._audit_token``), so a simple key=value scan is sufficient.
_AUDIT_MARKER = "api_request "
_KV_RE = re.compile(r"(\w+)=(\S+)")

_MONTHS = {
    m: f"{i:02d}"
    for i, m in enumerate(
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
        start=1,
    )
}

# 23.115.8.132 - - [02/Sep/2026:10:58:53 -0400] "GET /api/x?y=1 HTTP/2.0" 200 12 "-" "UA/1.0"
_ACCESS_RE = re.compile(
    r'^(?P<ip>\S+) \S+ \S+ \[(?P<ts>[^\]]+)\] "(?P<request>[^"]*)" '
    r'(?P<status>\d{3}) \S+ "(?P<referer>[^"]*)" "(?P<ua>[^"]*)"'
)


def _norm_nginx_time(raw: str) -> Optional[str]:
    """``02/Sep/2026:10:58:53 -0400`` -> ``2026-09-02 10:58:53`` (local).

    Both logs record host-local wall-clock time, so normalizing to the same
    string makes the legacy join a string comparison — no timezone maths and
    no per-line ``date`` subprocess.
    """
    stamp = raw.split()[0] if raw else ""
    parts = stamp.split(":")
    if len(parts) != 4:
        return None
    date_part = parts[0].split("/")
    if len(date_part) != 3:
        return None
    day, mon, year = date_part
    month = _MONTHS.get(mon)
    if month is None:
        return None
    return f"{year}-{month}-{day} {parts[1]}:{parts[2]}:{parts[3]}"


def _norm_journal_time(token: str) -> Optional[str]:
    """``2026-09-02T10:58:53-0400`` -> ``2026-09-02 10:58:53`` (local)."""
    stamp = re.sub(r"[+-]\d{2}:?\d{2}$", "", token)
    if "T" not in stamp:
        return None
    return stamp.replace("T", " ", 1)


def read_audit_lines(unit: str, since: str) -> List[Dict[str, str]]:
    """Pull ``api_request`` audit records out of the journal.

    ``since`` is an absolute local ``YYYY-MM-DD HH:MM:SS`` cutoff — the same
    string used to filter the access log, so both sides of the join are
    bounded identically. (Passing journalctl a relative expression instead
    would let systemd's own time parser pick a slightly different cutoff.)
    """
    try:
        proc = subprocess.run(
            [
                "journalctl",
                "-u",
                unit,
                "--since",
                since,
                "-o",
                "short-iso",
                "--no-pager",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        raise SystemExit("journalctl not found — run this on the API host.")
    if proc.returncode != 0 and not proc.stdout:
        raise SystemExit(
            f"journalctl -u {unit} failed (rc={proc.returncode}): "
            f"{proc.stderr.strip() or 'no output'}\n"
            "Reading another unit's journal usually needs sudo."
        )

    records: List[Dict[str, str]] = []
    for line in proc.stdout.splitlines():
        marker = line.find(_AUDIT_MARKER)
        if marker < 0:
            continue
        fields = dict(_KV_RE.findall(line[marker:]))
        if "method" not in fields or "path" not in fields:
            continue
        when = _norm_journal_time(line.split(None, 1)[0])
        if when is None:
            continue
        fields["_when"] = when
        records.append(fields)
    return records


def read_access_log(path: str, since: str) -> List[Dict[str, str]]:
    """Parse nginx access-log rows at or after ``since`` (local ISO string)."""
    rows: List[Dict[str, str]] = []
    try:
        handle = open(path, "r", errors="replace")
    except FileNotFoundError:
        raise SystemExit(f"access log not found: {path}")
    except PermissionError:
        raise SystemExit(f"cannot read {path} — try sudo.")
    with handle:
        for line in handle:
            match = _ACCESS_RE.match(line)
            if not match:
                continue
            when = _norm_nginx_time(match.group("ts"))
            if when is None or when < since:
                continue
            request = match.group("request").split()
            if len(request) < 2:
                continue
            rows.append(
                {
                    "_when": when,
                    "ip": match.group("ip"),
                    "method": request[0],
                    # The audit line records the path without its query
                    # string, so drop it here too or the join never matches.
                    "path": request[1].split("?", 1)[0],
                    "status": match.group("status"),
                    "ua": match.group("ua"),
                }
            )
    return rows


# ---------------------------------------------------------------------------
# Attribution
# ---------------------------------------------------------------------------


# An address is awarded to a caller only on this much agreement. _MIN_SUPPORT
# stops one lucky pairing from claiming an address outright — the shape a
# bypassing request takes when it lands on a quiet second. _MIN_SHARE keeps a
# genuinely mixed address (two customers behind one office NAT) unattributed
# rather than handed to whichever is busier.
_MIN_SUPPORT = 5
_MIN_SHARE = 0.9


def _join_key(when: str, method: str, path: str, status: str) -> str:
    return f"{when}|{method}|{path}|{status}"


def attribute(
    audit: List[Dict[str, str]], access: List[Dict[str, str]]
) -> Tuple[List[Dict[str, Any]], str, int]:
    """Pair each audit record with a client IP.

    Returns ``(records, mode, dropped)`` where each record carries at least
    ``ip`` and the identity fields, ``mode`` is ``"direct"`` or ``"legacy"``,
    and ``dropped`` counts requests that could not be attributed: ambiguous
    join keys in legacy mode, and lines with no ``client_ip`` in direct mode.

    A single ``client_ip`` anywhere in the window selects direct mode. For a
    window spanning the API restart that introduced the field, the journal
    holds both shapes; the older lines are then counted as dropped rather
    than run through the weaker join, so one report never mixes an exact
    attribution with an inferred one.
    """
    has_client_ip = any(r.get("client_ip", "-") not in ("-", None) for r in audit)

    if has_client_ip:
        out = []
        for rec in audit:
            ip = rec.get("client_ip", "-")
            if ip == "-":
                continue
            enriched = dict(rec)
            enriched["ip"] = ip
            out.append(enriched)
        return out, "direct", len(audit) - len(out)

    # Legacy: decide who owns each ADDRESS from aggregate evidence, rather
    # than trying to pair each request individually.
    #
    # Per-request pairing looked obvious and was wrong twice over. Requiring
    # a unique client IP per (second, method, path, status) let the website
    # BFF — which calls uvicorn at 127.0.0.1 directly, bypassing nginx
    # (deploy/API_BEHIND_CLOUDFLARE.md), so its lines have no access-log row
    # at all — adopt whichever customer shared that second. Tightening it to
    # a strict 1:1 count, then to a +/-1s window, cured that case but made
    # the failure worse in kind: a busy customer's own requests collide with
    # each other and drop, while a bypassing record sitting alone on a quiet
    # second survives untouched. The guard was keeping precisely the records
    # most likely to be wrong.
    #
    # The signal that actually identifies a caller is volume, which is how a
    # human reads this report: 8,504 requests from one address under one
    # User-Agent is the customer; nine scattered ones are noise. So each
    # cheaply-pairable request casts a VOTE for (caller, address), and an
    # address is awarded to a caller only on a decisive majority backed by
    # real support. Individual mispairings are then just noise in a tally
    # they cannot win.
    #
    # This is still a heuristic and cannot become sound — it never sees a
    # request that skipped nginx. Restarting the API puts client_ip on the
    # line and retires all of it.
    ips_by_key: Dict[str, set] = defaultdict(set)
    for row in access:
        ips_by_key[_join_key(row["_when"], row["method"], row["path"], row["status"])].add(
            row["ip"]
        )

    votes: Dict[str, Counter] = defaultdict(Counter)
    for rec in audit:
        key = _join_key(rec["_when"], rec["method"], rec["path"], rec.get("status", ""))
        candidates = ips_by_key.get(key)
        # Only unambiguous pairings vote. They are plentiful, and a request
        # that could have come from either of two clients says nothing about
        # which one owns the address.
        if candidates and len(candidates) == 1:
            votes[next(iter(candidates))][rec.get("caller_user_id", "-")] += 1

    owner_of: Dict[str, str] = {}
    for ip, tally in votes.items():
        total = sum(tally.values())
        caller, count = tally.most_common(1)[0]
        if total >= _MIN_SUPPORT and count / total >= _MIN_SHARE:
            owner_of[ip] = caller

    out = []
    dropped = 0
    for rec in audit:
        key = _join_key(rec["_when"], rec["method"], rec["path"], rec.get("status", ""))
        candidates = ips_by_key.get(key)
        if not candidates or len(candidates) != 1:
            dropped += 1
            continue
        ip = next(iter(candidates))
        if owner_of.get(ip) != rec.get("caller_user_id", "-"):
            # Either the address never earned an owner, or this record
            # disagrees with the one it earned — the shape both production
            # leaks took.
            dropped += 1
            continue
        enriched = dict(rec)
        enriched["ip"] = ip
        out.append(enriched)
    return out, "legacy", dropped


def _caller_id(rec: Dict[str, str]) -> Tuple[str, str, str, str]:
    """Group key: one row per distinct authenticated identity."""
    return (
        rec.get("caller_kind", "-"),
        rec.get("caller_user_id", "-"),
        rec.get("caller_key_id", "-"),
        rec.get("caller_name", "-"),
    )


def build_report(
    attributed: List[Dict[str, str]], access: List[Dict[str, str]]
) -> List[Dict[str, Any]]:
    """Collapse attributed requests into one entry per caller identity.

    Grouping is per KEY, not per owner: one owner running two keys is two
    rows (so you can see which to rotate), and one key used from two hosts
    is a single row listing both IPs.

    ``user_agents`` is an IP-level attribute, counted over every access-log
    row from that caller's IPs — the audit line has no User-Agent and the
    access log has no key, so there is no per-request pairing to be had.
    The counts therefore describe the IPs, and should two keys ever share an
    address both rows will list the union. Read them as "what runs at this
    address", not as a per-caller request tally (``requests`` is that).
    """
    ua_by_ip: Dict[str, Counter] = defaultdict(Counter)
    for row in access:
        ua_by_ip[row["ip"]][row["ua"]] += 1

    grouped: Dict[Tuple[str, str, str, str], List[Dict[str, str]]] = defaultdict(list)
    for rec in attributed:
        grouped[_caller_id(rec)].append(rec)

    report: List[Dict[str, Any]] = []
    for (kind, user_id, key_id, name), recs in grouped.items():
        ips = Counter(r["ip"] for r in recs)
        statuses = Counter(r.get("status", "-") for r in recs)
        paths = Counter(r["path"] for r in recs)
        end_users = Counter(
            r.get("end_user_id", "-") for r in recs if r.get("end_user_id", "-") != "-"
        )
        agents: Counter = Counter()
        for ip in ips:
            agents.update(ua_by_ip.get(ip, Counter()))
        report.append(
            {
                "caller_kind": kind,
                "caller_user_id": user_id,
                "caller_key_id": key_id,
                "caller_name": name,
                "requests": len(recs),
                "first_seen": min(r["_when"] for r in recs),
                "last_seen": max(r["_when"] for r in recs),
                "ips": ips.most_common(),
                "user_agents": agents.most_common(),
                "statuses": sorted(statuses.items()),
                "top_paths": paths.most_common(8),
                "end_users": end_users.most_common(5),
            }
        )
    report.sort(key=lambda e: e["requests"], reverse=True)
    return report


# ---------------------------------------------------------------------------
# api_keys enrichment (optional — the report is useful without a DB)
# ---------------------------------------------------------------------------


async def _fetch_keys(user_ids: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    """Look up key rows for the owners we saw. Keyed by ``str(id)``."""
    import asyncpg  # imported lazily so --no-db works without the dep

    from src.database.password_providers import resolve_db_credentials

    wanted = [u for u in user_ids if u and u not in ("-", "static", "anonymous")]
    if not wanted:
        return {}
    creds = resolve_db_credentials()
    conn = await asyncpg.connect(
        host=creds["host"],
        port=int(creds["port"]),
        database=creds["database"],
        user=creds["user"],
        password=creds["password"],
    )
    try:
        rows = await conn.fetch(
            """
            SELECT id, user_id, name, prefix, scopes,
                   created_at, last_used_at, revoked_at
            FROM api_keys
            WHERE user_id = ANY($1::text[])
            ORDER BY id
            """,
            wanted,
        )
    finally:
        await conn.close()
    return {
        str(r["id"]): {
            "id": r["id"],
            "user_id": r["user_id"],
            "name": r["name"],
            "prefix": r["prefix"],
            "scopes": list(r["scopes"] or []),
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "last_used_at": r["last_used_at"].isoformat() if r["last_used_at"] else None,
            "revoked": r["revoked_at"] is not None,
        }
        for r in rows
    }


def enrich_with_keys(report: List[Dict[str, Any]]) -> Optional[str]:
    """Attach ``key`` details in place. Returns a warning string on failure."""
    try:
        keys = asyncio.run(_fetch_keys({e["caller_user_id"] for e in report}))
    except Exception as exc:  # noqa: BLE001 — DB is a bonus, never a blocker
        return f"api_keys lookup unavailable ({type(exc).__name__}: {exc})"
    for entry in report:
        entry["key"] = keys.get(entry["caller_key_id"])
        # An owner's other keys are useful context when deciding what to
        # rotate, so surface any that did not appear in this window.
        entry["other_keys_for_owner"] = [
            k
            for k in keys.values()
            if k["user_id"] == entry["caller_user_id"] and str(k["id"]) != entry["caller_key_id"]
        ]
    return None


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _fmt_counter(pairs: List[Tuple[str, int]], limit: int = 6) -> str:
    shown = pairs[:limit]
    text = ", ".join(f"{name} ({count})" for name, count in shown)
    if len(pairs) > limit:
        text += f", +{len(pairs) - limit} more"
    return text or "-"


def render(report: List[Dict[str, Any]], mode: str, dropped: int, window: str) -> str:
    lines: List[str] = []
    lines.append(f"API caller attribution — {window}")
    if mode == "direct":
        lines.append("source: audit-log client_ip (exact, per request)")
    else:
        lines.append(
            "source: LEGACY join of nginx+audit on (second, method, path, status).\n"
            "        The API has not restarted since client_ip was added to the audit\n"
            "        line, so attribution is inferred. Ambiguous requests are dropped,\n"
            "        never guessed — an IP missing below was not necessarily anonymous."
        )
    if dropped:
        lines.append(f"unattributed requests: {dropped}")
        if mode == "direct":
            lines.append(
                "  (audit lines with no client_ip — emitted before the API last " "restarted)"
            )
    lines.append("")

    if not report:
        lines.append("(no attributable requests in this window)")
        return "\n".join(lines)

    for entry in report:
        header = f"{entry['caller_user_id']}"
        if entry["caller_name"] != "-":
            header += f"  key={entry['caller_name']} (id {entry['caller_key_id']})"
        elif entry["caller_key_id"] != "-":
            header += f"  key id {entry['caller_key_id']}"
        lines.append("=" * 74)
        lines.append(header)
        lines.append(f"  kind        : {entry['caller_kind']}")
        lines.append(f"  requests    : {entry['requests']}")
        lines.append(f"  window      : {entry['first_seen']} .. {entry['last_seen']}")
        lines.append(f"  client IPs  : {_fmt_counter(entry['ips'])}")
        lines.append(f"  user-agents : {_fmt_counter(entry['user_agents'], 4)}")
        lines.append(f"  statuses    : {_fmt_counter(entry['statuses'])}")
        lines.append(f"  top paths   : {_fmt_counter(entry['top_paths'], 5)}")
        if entry["end_users"]:
            lines.append(f"  end-users   : {_fmt_counter(entry['end_users'], 5)}")
        key = entry.get("key")
        if key:
            state = "REVOKED" if key["revoked"] else "active"
            lines.append(
                f"  key row     : prefix={key['prefix']}… {state} "
                f"scopes={','.join(key['scopes']) or '-'}"
            )
            lines.append(
                f"                created={key['created_at']} last_used={key['last_used_at']}"
            )
        others = entry.get("other_keys_for_owner") or []
        if others:
            # In legacy mode caller_key_id is unknown, so these are not
            # "other" keys — they are every key the owner has, one of which
            # served these requests.
            label = (
                "  owner's keys (pre-restart lines do not record which was used): "
                if entry["caller_key_id"] == "-"
                else "  owner's other keys: "
            )
            lines.append(
                label
                + ", ".join(
                    f"{k['name']} (id {k['id']}, " f"{'revoked' if k['revoked'] else 'active'})"
                    for k in others
                )
            )
    lines.append("=" * 74)
    lines.append(
        "user-agents are counted per client IP (no per-request pairing exists), "
        "so they\ndescribe the address, not this caller's request mix."
    )
    lines.append("Revoke a key with:  make api-keys-revoke ID=<caller_key_id>")
    lines.append("List an owner's keys with:  make api-keys-list KEY_USER=<caller_user_id>")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.tools.api_caller_report",
        description="Attribute API traffic to the key and owner that authenticated it.",
    )
    parser.add_argument("--hours", type=float, default=2.0, help="lookback window (default 2)")
    parser.add_argument("--unit", default="zerogex-oa-api", help="systemd unit to read")
    parser.add_argument(
        "--access-log", default="/var/log/nginx/access.log", help="nginx access log path"
    )
    parser.add_argument(
        "--ip", action="append", default=[], help="filter to client IP (repeatable)"
    )
    parser.add_argument("--user", action="append", default=[], help="filter to caller_user_id")
    parser.add_argument("--key-id", action="append", default=[], help="filter to caller_key_id")
    parser.add_argument(
        "--ua-contains", default=None, help="filter to callers whose User-Agent contains this"
    )
    parser.add_argument("--no-db", action="store_true", help="skip the api_keys lookup")
    parser.add_argument("--json", dest="json_out", default=None, help="also write JSON here")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = _build_parser().parse_args(argv)

    since_dt = datetime.now() - timedelta(hours=args.hours)
    since = since_dt.strftime("%Y-%m-%d %H:%M:%S")
    window = f"last {args.hours:g}h (since {since})"

    audit = read_audit_lines(args.unit, since)
    if not audit:
        print(
            f"No 'api_request' audit lines in the last {args.hours:g}h of "
            f"'journalctl -u {args.unit}'.\n"
            "The journal is capped and vacuumed nightly "
            "(setup/systemd/zerogex-oa-journald.conf), so history is short — try a\n"
            "smaller --hours, and confirm the API runs with LOG_LEVEL=INFO.",
            file=sys.stderr,
        )
        return 2

    access = read_access_log(args.access_log, since)
    attributed, mode, dropped = attribute(audit, access)

    if args.ip:
        wanted_ips = set(args.ip)
        attributed = [r for r in attributed if r["ip"] in wanted_ips]
    if args.user:
        wanted_users = set(args.user)
        attributed = [r for r in attributed if r.get("caller_user_id") in wanted_users]
    if args.key_id:
        if mode == "legacy":
            print(
                "--key-id cannot match: audit lines written before the API last "
                "restarted\ncarry no caller_key_id. Filter by --user instead, or "
                "restart the API so\nthe field starts being recorded.",
                file=sys.stderr,
            )
            return 2
        wanted_keys = {str(k) for k in args.key_id}
        attributed = [r for r in attributed if r.get("caller_key_id") in wanted_keys]

    report = build_report(attributed, access)

    if args.ua_contains:
        needle = args.ua_contains.lower()
        report = [e for e in report if any(needle in ua.lower() for ua, _ in e["user_agents"])]

    warning = None
    if not args.no_db and report:
        warning = enrich_with_keys(report)

    print(render(report, mode, dropped, window))
    if warning:
        print(f"\nnote: {warning}", file=sys.stderr)

    if args.json_out:
        with open(args.json_out, "w") as fh:
            json.dump(
                {"window": window, "mode": mode, "unattributed": dropped, "callers": report},
                fh,
                indent=2,
                default=str,
            )
        print(f"\nJSON written to {args.json_out}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
