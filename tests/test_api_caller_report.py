"""Tests for src.tools.api_caller_report.

The tool answers "which API key is this IP using" by reading two logs that
each hold half the answer. The properties that matter:

- Both timestamp formats normalize to the same local-time string, which is
  what makes the legacy join a string comparison.
- Direct mode (audit lines carry ``client_ip``) attributes exactly.
- Legacy mode (pre-deploy audit lines) DISCARDS ambiguous requests instead
  of guessing — a wrong attribution here would point an investigation at
  the wrong customer, which is worse than no answer.
- Callers are grouped per KEY, not per owner, so one owner running two keys
  shows as two rows and one key used from two hosts shows as one row.
"""

from __future__ import annotations

import os

import pytest

from src.tools.api_caller_report import (
    _norm_journal_time,
    _norm_nginx_time,
    _record_filter,
    _status_matcher,
    attribute,
    build_report,
    is_direct_mode,
    read_access_log,
    read_audit_lines,
)

ACCESS_LINES = [
    # Two clients hit the same path in the same second -> ambiguous join key.
    '1.1.1.1 - - [02/Sep/2026:10:58:53 -0400] "GET /api/gex/summary?symbol=SPY HTTP/2.0" '
    '200 559 "-" "alpha-client/1.0"',
    '2.2.2.2 - - [02/Sep/2026:10:58:53 -0400] "GET /api/gex/summary?symbol=QQQ HTTP/2.0" '
    '200 559 "-" "beta-client/1.0"',
    # Unique to 1.1.1.1 -> clean join key.
    '1.1.1.1 - - [02/Sep/2026:10:58:54 -0400] "GET /api/forecast?symbol=SPX HTTP/2.0" '
    '200 704 "-" "alpha-client/1.0"',
]


def _audit(when, path, status="200", **fields):
    rec = {"_when": when, "method": "GET", "path": path, "status": status}
    rec.update(fields)
    return rec


class TestTimestampNormalisation:
    def test_nginx_and_journal_agree(self):
        """The join only works if both sides land on the same string."""
        assert _norm_nginx_time("02/Sep/2026:10:58:53 -0400") == "2026-09-02 10:58:53"
        assert _norm_journal_time("2026-09-02T10:58:53-0400") == "2026-09-02 10:58:53"
        # Some journalctl builds emit a colon in the offset.
        assert _norm_journal_time("2026-09-02T10:58:53-04:00") == "2026-09-02 10:58:53"

    def test_malformed_input_returns_none(self):
        assert _norm_nginx_time("") is None
        assert _norm_nginx_time("not-a-timestamp") is None
        assert _norm_nginx_time("02/Xxx/2026:10:58:53 -0400") is None
        assert _norm_journal_time("2026-09-02 10:58:53") is None


class TestAccessLogParsing:
    def test_parses_ip_path_and_agent(self, tmp_path):
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")

        rows = read_access_log(str(log), since="2026-09-02 00:00:00")

        assert len(rows) == 3
        assert rows[0]["ip"] == "1.1.1.1"
        assert rows[0]["ua"] == "alpha-client/1.0"
        assert rows[0]["status"] == "200"
        # The query string must be dropped: the audit line logs path only, so
        # keeping it here would make every join key miss.
        assert rows[0]["path"] == "/api/gex/summary"

    def test_window_filter_excludes_older_rows(self, tmp_path):
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")

        rows = read_access_log(str(log), since="2026-09-02 10:58:54")

        assert [r["path"] for r in rows] == ["/api/forecast"]

    def test_ignores_unparseable_lines(self, tmp_path):
        log = tmp_path / "access.log"
        log.write_text("garbage\n" + ACCESS_LINES[0] + "\n\n")

        assert len(read_access_log(str(log), since="2026-09-02 00:00:00")) == 1


class TestDirectMode:
    def test_client_ip_is_used_when_present(self, tmp_path):
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")
        access = read_access_log(str(log), since="2026-09-02 00:00:00")
        audit = [
            _audit(
                "2026-09-02 10:58:53",
                "/api/gex/summary",
                client_ip="1.1.1.1",
                caller_user_id="alice@example.com",
                caller_key_id="7",
            )
        ]

        attributed, mode, dropped = attribute(audit, access)

        assert mode == "direct"
        assert dropped == 0
        assert attributed[0]["ip"] == "1.1.1.1"

    def test_ambiguity_does_not_matter_in_direct_mode(self, tmp_path):
        """The same-second collision that defeats the legacy join is a
        non-issue once the IP is on the line itself."""
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")
        access = read_access_log(str(log), since="2026-09-02 00:00:00")
        audit = [
            _audit("2026-09-02 10:58:53", "/api/gex/summary", client_ip="1.1.1.1"),
            _audit("2026-09-02 10:58:53", "/api/gex/summary", client_ip="2.2.2.2"),
        ]

        attributed, mode, dropped = attribute(audit, access)

        assert mode == "direct"
        assert dropped == 0
        assert {r["ip"] for r in attributed} == {"1.1.1.1", "2.2.2.2"}

    def test_mixed_journal_drops_pre_restart_lines(self, tmp_path):
        """A window spanning the restart holds both line shapes.

        The older lines are dropped and counted, not silently run through the
        weaker join — one report should not mix an exact attribution with an
        inferred one.
        """
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")
        access = read_access_log(str(log), since="2026-09-02 00:00:00")
        audit = [
            _audit("2026-09-02 10:58:54", "/api/forecast"),  # pre-restart
            _audit("2026-09-02 10:58:53", "/api/gex/summary", client_ip="1.1.1.1"),
        ]

        attributed, mode, dropped = attribute(audit, access)

        assert mode == "direct"
        assert dropped == 1
        assert [r["ip"] for r in attributed] == ["1.1.1.1"]


class TestLegacyMode:
    """Legacy mode awards an ADDRESS to a caller on aggregate evidence.

    Each cheaply-pairable request votes for (caller, address); an address is
    awarded only on a decisive majority with real support behind it. These
    pin the four ways the naive per-request join went wrong.
    """

    @staticmethod
    def _traffic(ip, ua, path, caller, start=10, n=10):
        """n requests from one client: matching access rows and audit lines."""
        rows = [
            f"{ip} - - [02/Sep/2026:10:59:{s:02d} -0400] "
            f'"GET {path}?x=1 HTTP/2.0" 200 100 "-" "{ua}"'
            for s in range(start, start + n)
        ]
        audit = [
            _audit(f"2026-09-02 10:59:{s:02d}", path, caller_user_id=caller)
            for s in range(start, start + n)
        ]
        return rows, audit

    def test_a_consistent_client_is_attributed(self, tmp_path):
        rows, audit = self._traffic("1.1.1.1", "cust/1.0", "/api/owned", "cust@x")
        log = tmp_path / "access.log"
        log.write_text("\n".join(rows) + "\n")
        access = read_access_log(str(log), since="2026-09-02 00:00:00")

        attributed, mode, dropped = attribute(audit, access)

        assert mode == "legacy"
        assert dropped == 0
        assert {r["ip"] for r in attributed} == {"1.1.1.1"}
        assert {r["caller_user_id"] for r in attributed} == {"cust@x"}

    def test_ambiguous_second_does_not_vote(self, tmp_path):
        """Two clients on the same (second, path, status) say nothing about
        which owns either address, so neither request counts."""
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")
        access = read_access_log(str(log), since="2026-09-02 00:00:00")
        audit = [_audit("2026-09-02 10:58:53", "/api/gex/summary", caller_user_id="alice@x")]

        attributed, mode, dropped = attribute(audit, access)

        assert mode == "legacy"
        assert attributed == []
        assert dropped == 1

    def test_bff_minority_cannot_take_a_customers_address(self, tmp_path):
        """The production leak. The website BFF calls uvicorn at 127.0.0.1
        directly, bypassing nginx (deploy/API_BEHIND_CLOUDFLARE.md), so its
        lines have no access-log row and land on whichever customer shared
        the second. Against that customer's own volume it must lose."""
        # Proportions matter: in production the contamination was ~1.5% of
        # the address's traffic (6 website requests against lori's 399), and
        # _MIN_SHARE is set for that. A fixture with 17% noise would be
        # testing the threshold, not the behaviour.
        rows, audit = self._traffic("1.1.1.1", "cust/1.0", "/api/owned", "cust@x", n=30)
        log = tmp_path / "access.log"
        log.write_text("\n".join(rows) + "\n")
        access = read_access_log(str(log), since="2026-09-02 00:00:00")
        # Two bypassing records colliding with the customer's seconds.
        audit += [
            _audit(
                "2026-09-02 10:59:11",
                "/api/owned",
                caller_user_id="zerogex-web",
                end_user_id="user_a",
            ),
            _audit(
                "2026-09-02 10:59:12",
                "/api/owned",
                caller_user_id="zerogex-web",
                end_user_id="user_b",
            ),
        ]

        attributed, mode, dropped = attribute(audit, access)

        assert mode == "legacy"
        owners = {r["caller_user_id"] for r in attributed}
        assert owners == {"cust@x"}, f"1.1.1.1 belongs to cust@x, got {owners}"
        assert dropped == 2

    def test_a_lone_pairing_cannot_claim_an_address(self, tmp_path):
        """The subtler leak, which survived the strict 1:1 join.

        nginx stamps $time_local at completion and the audit line is emitted
        in the middleware's finally, so one request's two records can
        straddle a second. That orphans the access-log row, and a bypassing
        record sitting on it pairs perfectly. Support alone refutes it: one
        vote is not evidence of ownership."""
        log = tmp_path / "access.log"
        log.write_text(
            "9.9.9.9 - - [02/Sep/2026:10:58:53 -0400] "
            '"GET /api/trade-bias?u=QQQ HTTP/2.0" 200 899 "-" "cust/1.0"\n'
        )
        access = read_access_log(str(log), since="2026-09-02 00:00:00")
        audit = [
            # The row's real owner, logged a second later by the app.
            _audit("2026-09-02 10:58:54", "/api/trade-bias", caller_user_id="cust@x"),
            # Bypassing, with no row of its own, landing on the orphan.
            _audit(
                "2026-09-02 10:58:53",
                "/api/trade-bias",
                caller_user_id="zerogex-web",
                end_user_id="user_abc",
            ),
        ]

        attributed, mode, dropped = attribute(audit, access)

        assert mode == "legacy"
        assert attributed == [], "a single pairing claimed an address outright"
        assert dropped == 2

    def test_a_genuinely_shared_address_is_left_unattributed(self, tmp_path):
        """Two customers behind one NAT is not a majority for either, so the
        address earns no owner rather than going to whoever is busier."""
        rows_a, audit_a = self._traffic("7.7.7.7", "a/1.0", "/api/a", "a@x", start=10, n=6)
        rows_b, audit_b = self._traffic("7.7.7.7", "b/1.0", "/api/b", "b@x", start=30, n=6)
        log = tmp_path / "access.log"
        log.write_text("\n".join(rows_a + rows_b) + "\n")
        access = read_access_log(str(log), since="2026-09-02 00:00:00")

        attributed, mode, dropped = attribute(audit_a + audit_b, access)

        assert mode == "legacy"
        assert attributed == []
        assert dropped == 12

    def test_request_with_no_access_log_match_is_dropped(self, tmp_path):
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")
        access = read_access_log(str(log), since="2026-09-02 00:00:00")
        audit = [_audit("2026-09-02 11:00:00", "/api/nowhere", caller_user_id="alice@x")]

        attributed, _, dropped = attribute(audit, access)

        assert attributed == []
        assert dropped == 1


class TestReportGrouping:
    def test_one_key_used_from_two_ips_is_one_row(self, tmp_path):
        """The shared-key-across-two-hosts case the investigation turned up."""
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")
        access = read_access_log(str(log), since="2026-09-02 00:00:00")
        attributed = [
            _audit(
                "2026-09-02 10:58:53",
                "/api/gex/summary",
                ip="1.1.1.1",
                caller_kind="db",
                caller_user_id="alice@x",
                caller_key_id="7",
                caller_name="shared",
            ),
            _audit(
                "2026-09-02 10:58:53",
                "/api/gex/summary",
                ip="2.2.2.2",
                caller_kind="db",
                caller_user_id="alice@x",
                caller_key_id="7",
                caller_name="shared",
            ),
        ]

        report = build_report(attributed, access)

        assert len(report) == 1
        assert report[0]["requests"] == 2
        assert dict(report[0]["ips"]) == {"1.1.1.1": 1, "2.2.2.2": 1}
        # User-Agents come from the access log, joined by IP.
        assert dict(report[0]["user_agents"]) == {"alpha-client/1.0": 2, "beta-client/1.0": 1}

    def test_one_owner_with_two_keys_is_two_rows(self, tmp_path):
        """Grouping by owner alone would hide which key to rotate."""
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")
        access = read_access_log(str(log), since="2026-09-02 00:00:00")
        attributed = [
            _audit(
                "2026-09-02 10:58:53",
                "/api/gex/summary",
                ip="1.1.1.1",
                caller_kind="db",
                caller_user_id="gexa@x",
                caller_key_id="15",
                caller_name="hud",
            ),
            _audit(
                "2026-09-02 10:58:53",
                "/api/gex/summary",
                ip="2.2.2.2",
                caller_kind="db",
                caller_user_id="gexa@x",
                caller_key_id="16",
                caller_name="simbridge",
            ),
        ]

        report = build_report(attributed, access)

        assert len(report) == 2
        assert {e["caller_key_id"] for e in report} == {"15", "16"}

    def test_rows_are_ranked_by_volume(self, tmp_path):
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")
        access = read_access_log(str(log), since="2026-09-02 00:00:00")
        attributed = [
            _audit("2026-09-02 10:58:53", "/api/a", ip="1.1.1.1", caller_key_id="1"),
            _audit("2026-09-02 10:58:54", "/api/a", ip="1.1.1.1", caller_key_id="1"),
            _audit("2026-09-02 10:58:53", "/api/b", ip="2.2.2.2", caller_key_id="2"),
        ]

        report = build_report(attributed, access)

        assert [e["requests"] for e in report] == [2, 1]


class TestStatusFilter:
    """``--status`` turns the report into "who is being refused, and for what".

    The motivating case: scope enforcement went on, a plain
    ``grep status=403`` over three days of journal returned more lines than
    anyone can read, and every line differed by ``duration_ms`` and
    ``client_ip`` so ``uniq -c`` counted everything once. Filtering the
    records and letting the existing per-caller grouping do the counting is
    the answer — it also joins each caller to their key's SCOPES, which is
    what says which grant is missing.
    """

    def test_exact_code(self):
        matches = _status_matcher(["403"])
        assert matches({"status": "403"})
        assert not matches({"status": "200"})
        assert not matches({"status": "404"})

    def test_class_shorthand_covers_the_family(self):
        matches = _status_matcher(["4xx"])
        assert matches({"status": "401"})
        assert matches({"status": "403"})
        assert matches({"status": "429"})
        assert not matches({"status": "200"})
        assert not matches({"status": "503"})

    def test_codes_and_classes_combine(self):
        matches = _status_matcher(["403", "5xx"])
        assert matches({"status": "403"})
        assert matches({"status": "502"})
        assert not matches({"status": "401"})

    def test_missing_status_never_matches(self):
        """A legacy line with no status must not be swept into a class filter
        — it would inflate the count of an incident it has no evidence of."""
        matches = _status_matcher(["4xx"])
        assert not matches({})
        assert not matches({"status": ""})
        assert not matches({"status": "-"})

    def test_a_bad_value_is_a_usage_error_not_an_empty_report(self):
        """The failure mode worth preventing: a typo that matches nothing,
        and an operator concluding there were no 403s."""
        for bad in ("4o3", "forbidden", "40", "xx4", ""):
            with pytest.raises(ValueError):
                _status_matcher([bad])

    def test_filtered_records_still_group_and_count_per_caller(self, tmp_path):
        """End of the pipeline: the filter feeds the existing grouping, so the
        answer arrives as one row per key with its failing paths."""
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")
        access = read_access_log(str(log), since="2026-09-02 00:00:00")

        records = [
            _audit(
                "2026-09-02 10:58:53",
                "/api/market/open-interest",
                status="403",
                ip="1.1.1.1",
                caller_kind="db",
                caller_user_id="jim@x",
                caller_key_id="145",
                caller_name="jim-2",
            ),
            _audit(
                "2026-09-02 10:58:54",
                "/api/market/open-interest",
                status="403",
                ip="1.1.1.1",
                caller_kind="db",
                caller_user_id="jim@x",
                caller_key_id="145",
                caller_name="jim-2",
            ),
            # Same caller, succeeding elsewhere — must not be counted.
            _audit(
                "2026-09-02 10:58:55",
                "/api/gex/summary",
                status="200",
                ip="1.1.1.1",
                caller_kind="db",
                caller_user_id="jim@x",
                caller_key_id="145",
                caller_name="jim-2",
            ),
        ]

        matches = _status_matcher(["403"])
        report = build_report([r for r in records if matches(r)], access)

        assert len(report) == 1
        assert report[0]["requests"] == 2
        assert report[0]["caller_key_id"] == "145"
        assert dict(report[0]["top_paths"]) == {"/api/market/open-interest": 2}


AUDIT_LINE = (
    "{ts} ip-1 zerogex-oa-api[1]: {ts_app} - src.api.audit - INFO - "
    "[request_id=r{n}] api_request method=GET path={path} status={status} "
    "client_ip={ip} caller_kind=db caller_user_id={user} caller_key_id={key} "
    "caller_name=k{key} end_user_id=- duration_ms=1.0"
)


def _fake_journalctl(tmp_path, monkeypatch, lines):
    """Put a stub ``journalctl`` on PATH that prints ``lines``.

    Exercises the real subprocess path — the streaming rewrite is about how
    the pipe is consumed, so stubbing ``Popen`` would test the wrong thing.
    """
    bindir = tmp_path / "bin"
    bindir.mkdir()
    payload = tmp_path / "journal.txt"
    payload.write_text("\n".join(lines) + "\n")
    stub = bindir / "journalctl"
    stub.write_text("#!/bin/sh\ncat %s\n" % payload)
    stub.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bindir}:{os.environ['PATH']}")


class TestStreamingRead:
    """The journal read streams and filters as it goes.

    It used to buffer the whole window twice (``capture_output`` string, then
    ``splitlines`` list) before parsing a record. On 2026-09-03 that was
    OOM-killed on a 72h window: one caller retrying a 403 at ~38/s had made
    the journal too big to read, so the incident blocked its own
    investigation. Pushing the filter into the reader is what keeps an
    incident window to the failures.
    """

    def _lines(self):
        return [
            AUDIT_LINE.format(
                ts="2026-09-03T10:28:46-04:00",
                ts_app="2026-09-03 10:28:46,117",
                n=1,
                path="/api/gex/summary",
                status="200",
                ip="1.1.1.1",
                user="web",
                key="4",
            ),
            AUDIT_LINE.format(
                ts="2026-09-03T10:29:00-04:00",
                ts_app="2026-09-03 10:29:00,000",
                n=2,
                path="/api/market/open-interest",
                status="403",
                ip="2.2.2.2",
                user="jim@x",
                key="145",
            ),
            AUDIT_LINE.format(
                ts="2026-09-03T10:29:01-04:00",
                ts_app="2026-09-03 10:29:01,000",
                n=3,
                path="/api/option/quote",
                status="403",
                ip="3.3.3.3",
                user="rak@x",
                key="59",
            ),
            "2026-09-03T10:29:02-04:00 ip-1 zerogex-oa-api[1]: not an audit line",
        ]

    def test_reads_every_audit_record_when_unfiltered(self, tmp_path, monkeypatch):
        _fake_journalctl(tmp_path, monkeypatch, self._lines())
        records = read_audit_lines("zerogex-oa-api", "2026-09-03 00:00:00")
        assert [r["path"] for r in records] == [
            "/api/gex/summary",
            "/api/market/open-interest",
            "/api/option/quote",
        ]
        assert records[1]["caller_key_id"] == "145"
        assert records[1]["_when"] == "2026-09-03 10:29:00"

    def test_keep_predicate_discards_before_retention(self, tmp_path, monkeypatch):
        """The point of the pushdown: non-matching records are never held."""
        _fake_journalctl(tmp_path, monkeypatch, self._lines())
        records = read_audit_lines(
            "zerogex-oa-api", "2026-09-03 00:00:00", keep=_record_filter(["403"], [])
        )
        assert len(records) == 2
        assert {r["status"] for r in records} == {"403"}

    def test_path_and_status_filters_compose(self, tmp_path, monkeypatch):
        _fake_journalctl(tmp_path, monkeypatch, self._lines())
        records = read_audit_lines(
            "zerogex-oa-api",
            "2026-09-03 00:00:00",
            keep=_record_filter(["403"], ["/api/market/"]),
        )
        assert [r["path"] for r in records] == ["/api/market/open-interest"]


class TestRecordFilter:
    def test_no_filters_is_none_not_an_always_true_predicate(self):
        assert _record_filter([], []) is None

    def test_path_matches_on_prefix(self):
        keep = _record_filter([], ["/api/gex/premium_surface"])
        assert keep({"path": "/api/gex/premium_surface"})
        assert keep({"path": "/api/gex/premium_surface/extra"})
        assert not keep({"path": "/api/gex/vol_surface"})

    def test_several_prefixes_are_a_union(self):
        keep = _record_filter([], ["/api/option/", "/api/tools/"])
        assert keep({"path": "/api/option/quote"})
        assert keep({"path": "/api/tools/option-calculator"})
        assert not keep({"path": "/api/gex/summary"})

    def test_a_bad_status_still_raises_through_the_combiner(self):
        with pytest.raises(ValueError):
            _record_filter(["nope"], [])


class TestAccessLogScoping:
    def test_only_ips_keeps_just_those_addresses(self, tmp_path):
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")

        everything = read_access_log(str(log), since="2026-09-02 00:00:00")
        scoped = read_access_log(str(log), since="2026-09-02 00:00:00", only_ips={"1.1.1.1"})

        assert {r["ip"] for r in everything} == {"1.1.1.1", "2.2.2.2"}
        assert {r["ip"] for r in scoped} == {"1.1.1.1"}
        # Same rows for the address we kept — scoping must not alter content.
        assert [r for r in everything if r["ip"] == "1.1.1.1"] == scoped

    def test_user_agents_survive_scoping_in_direct_mode(self, tmp_path):
        """Scoping is only safe because it drops addresses the report never
        looks up. The UA tally for a listed caller must be unchanged."""
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")
        attributed = [
            _audit(
                "2026-09-02 10:58:53",
                "/api/gex/summary",
                ip="1.1.1.1",
                client_ip="1.1.1.1",
                caller_kind="db",
                caller_user_id="alice@x",
                caller_key_id="7",
                caller_name="k7",
            )
        ]
        scoped = read_access_log(str(log), since="2026-09-02 00:00:00", only_ips={"1.1.1.1"})
        report = build_report(attributed, scoped)
        assert dict(report[0]["user_agents"]) == {"alpha-client/1.0": 2}


class TestModeDetection:
    def test_one_client_ip_anywhere_selects_direct(self):
        assert is_direct_mode([{"client_ip": "-"}, {"client_ip": "1.1.1.1"}])

    def test_all_dashes_is_legacy(self):
        assert not is_direct_mode([{"client_ip": "-"}, {}])

    def test_empty_window_is_legacy(self):
        """No evidence of the field is not evidence of the field."""
        assert not is_direct_mode([])
