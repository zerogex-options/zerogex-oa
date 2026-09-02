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

from src.tools.api_caller_report import (
    _norm_journal_time,
    _norm_nginx_time,
    attribute,
    build_report,
    read_access_log,
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
    def test_unambiguous_request_is_attributed(self, tmp_path):
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")
        access = read_access_log(str(log), since="2026-09-02 00:00:00")
        audit = [_audit("2026-09-02 10:58:54", "/api/forecast", caller_user_id="alice@x")]

        attributed, mode, dropped = attribute(audit, access)

        assert mode == "legacy"
        assert dropped == 0
        assert attributed[0]["ip"] == "1.1.1.1"

    def test_ambiguous_request_is_dropped_not_guessed(self, tmp_path):
        """Two IPs share this (second, path, status); attributing it to
        either would be a coin flip pointed at a real customer."""
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")
        access = read_access_log(str(log), since="2026-09-02 00:00:00")
        audit = [_audit("2026-09-02 10:58:53", "/api/gex/summary", caller_user_id="alice@x")]

        attributed, mode, dropped = attribute(audit, access)

        assert mode == "legacy"
        assert attributed == []
        assert dropped == 1

    def test_bff_request_cannot_steal_another_clients_ip(self, tmp_path):
        """The website BFF calls uvicorn at 127.0.0.1 directly, bypassing
        nginx (deploy/API_BEHIND_CLOUDFLARE.md), so its requests produce an
        audit line with NO access-log row of their own.

        Such a line must not be paired with whatever unrelated client happens
        to share its (second, method, path, status). Observed in production:
        website traffic carrying end-user tokens was reported at an external
        customer's residential IP, because that IP owned the only nginx row
        for the second.
        """
        log = tmp_path / "access.log"
        log.write_text("\n".join(ACCESS_LINES) + "\n")
        access = read_access_log(str(log), since="2026-09-02 00:00:00")
        audit = [
            # The real owner of the only 10:58:54 /api/forecast nginx row.
            _audit("2026-09-02 10:58:54", "/api/forecast", caller_user_id="customer@x"),
            # A BFF-direct request that never touched nginx.
            _audit(
                "2026-09-02 10:58:54",
                "/api/forecast",
                caller_user_id="zerogex-web",
                end_user_id="user_abc",
            ),
        ]

        attributed, mode, dropped = attribute(audit, access)

        assert mode == "legacy"
        # One nginx row cannot substantiate two requests. Neither is claimed.
        assert attributed == []
        assert dropped == 2

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
