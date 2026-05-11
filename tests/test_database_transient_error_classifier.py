"""Unit tests for ``DatabaseManager._is_transient_db_error``.

Locks in the classifier semantics that grew out of the 2026-05-11 prod
incident: a per-statement ``command_timeout`` raises a bare
``TimeoutError``, which used to be classified as *transient* and trigger
a pool-wide reconnect.  That was wrong — the connection is fine, the
specific query was just slow — and produced concurrent reconnect storms
when several heavy queries (``/api/max-pain/current``) timed out in
parallel.  The classifier now only matches *connection/pool*-level
failures.
"""

from __future__ import annotations

import asyncio

import pytest

from src.api.database import DatabaseManager


def test_bare_timeout_error_is_NOT_transient():
    """Statement-level command_timeout must propagate as a normal error,
    not as a trigger for pool reconnect."""
    assert DatabaseManager._is_transient_db_error(TimeoutError()) is False
    assert DatabaseManager._is_transient_db_error(asyncio.TimeoutError()) is False


def test_error_message_containing_only_timeout_is_NOT_transient():
    """The bare word 'timeout' inside a message must not, on its own,
    classify the error as transient."""
    assert DatabaseManager._is_transient_db_error(RuntimeError("query timeout")) is False


def test_connection_level_errors_ARE_transient():
    """Connection/pool-level failures are the legitimate retry target."""
    for msg in (
        "connection reset by peer",
        "connection refused",
        "connection is closed",
        "pool is closed",
        "pool is closing",
        "ssl handshake failure",
        "ssl syscall error: EOF detected",
        "eof detected",
    ):
        assert DatabaseManager._is_transient_db_error(RuntimeError(msg)) is True, msg


def test_connection_error_subclasses_ARE_transient():
    """ConnectionError and OSError subclasses still qualify (network blips)."""
    assert DatabaseManager._is_transient_db_error(ConnectionResetError()) is True
    assert DatabaseManager._is_transient_db_error(ConnectionRefusedError()) is True
    assert DatabaseManager._is_transient_db_error(OSError("network unreachable")) is True


def test_unrelated_runtime_errors_are_NOT_transient():
    """Non-DB exceptions must propagate cleanly without triggering reconnect."""
    assert DatabaseManager._is_transient_db_error(RuntimeError("oops")) is False
    assert DatabaseManager._is_transient_db_error(ValueError("bad data")) is False
    assert DatabaseManager._is_transient_db_error(KeyError("missing")) is False
