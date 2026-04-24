#!/usr/bin/env python3
"""Idempotently inject the X-API-Key include into nginx ``location`` blocks
that proxy to the ZeroGEX API upstream.

For every nginx site config given on the command line, this:
  1. Walks every ``location`` block.
  2. If the block contains ``proxy_pass http://(localhost|127.0.0.1):<api-port>``,
     ensures the block also contains
     ``include /etc/nginx/conf.d/zerogex-api-key.conf*;``.
  3. Inserts the include right after the last ``proxy_set_header`` line in
     that block (or just before the closing brace if there are none).
  4. Leaves blocks that already include the file untouched.

Backups are written next to the original as ``<file>.bak-YYYYMMDD-HHMMSS``
the first time this run modifies them.

Exits 0 on success.  Exits non-zero if a file can't be parsed (mismatched
braces, etc.) — the deploy should fail loudly rather than write a broken
config.

Usage:
    nginx_inject_api_key.py <site-config> [<site-config> ...]

Designed for use from ``deploy/steps/125.api_auth``.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import re
import shutil
import sys
from pathlib import Path

INCLUDE_LINE = "include /etc/nginx/conf.d/zerogex-api-key.conf*;"
INCLUDE_NEEDLE = "zerogex-api-key.conf"  # match with or without trailing *
PROXY_HOSTS = (r"127\.0\.0\.1", r"localhost")
DEFAULT_PORT = 8000


def _make_proxy_re(port: int) -> re.Pattern[str]:
    hosts = "|".join(PROXY_HOSTS)
    return re.compile(rf"proxy_pass\s+https?://(?:{hosts}):{port}\b")


def _find_blocks(text: str) -> list[tuple[int, int]]:
    """Return (open_brace_idx, close_brace_idx) for each top-level ``location``
    block found in ``text``.

    "Top-level" here means the block's outer location keyword sits at
    indentation we don't try to interpret — we just bracket-match from
    the keyword's opening brace to its matching close.
    """
    blocks: list[tuple[int, int]] = []
    pos = 0
    location_re = re.compile(r"\blocation\b[^{]*\{")
    while True:
        m = location_re.search(text, pos)
        if not m:
            return blocks
        open_idx = m.end() - 1  # index of the '{'
        depth = 0
        i = open_idx
        while i < len(text):
            ch = text[i]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    blocks.append((open_idx, i))
                    pos = i + 1
                    break
            i += 1
        else:
            raise ValueError(
                f"Unbalanced braces starting at offset {open_idx}; "
                "refusing to edit the file."
            )


def _block_indent(text: str, open_idx: int) -> str:
    """Heuristic: indentation to use for new lines we insert inside a block.

    Look at the first non-blank line after the opening brace.  If we can't
    find one, fall back to four spaces.
    """
    nl = text.find("\n", open_idx)
    if nl < 0:
        return "    "
    j = nl + 1
    while j < len(text):
        line_end = text.find("\n", j)
        if line_end < 0:
            line_end = len(text)
        line = text[j:line_end]
        stripped = line.lstrip(" \t")
        if stripped and not stripped.startswith("}"):
            return line[: len(line) - len(stripped)]
        j = line_end + 1
    return "    "


def _patch(text: str, port: int) -> tuple[str, int]:
    """Return (new_text, num_blocks_modified)."""
    proxy_re = _make_proxy_re(port)
    blocks = _find_blocks(text)
    edits: list[tuple[int, str]] = []  # (insert_at_index, line_to_insert)

    for open_idx, close_idx in blocks:
        block_text = text[open_idx + 1 : close_idx]
        if not proxy_re.search(block_text):
            continue
        if INCLUDE_NEEDLE in block_text:
            continue

        indent = _block_indent(text, open_idx)
        # Insert just before the closing brace, on its own line.  Find the
        # last non-blank line within the block and insert after it.  This
        # keeps formatting predictable.
        insertion = f"\n{indent}# Auto-injected by deploy: nginx forwards X-API-Key to upstream.\n{indent}{INCLUDE_LINE}\n"

        # Trim any trailing whitespace + newline immediately before the
        # closing brace's line so the insertion sits cleanly above ``}``.
        # We compute the position of the start of the line containing
        # close_idx so we can insert before its leading whitespace.
        line_start = text.rfind("\n", 0, close_idx) + 1
        edits.append((line_start, insertion))

    if not edits:
        return text, 0

    # Apply edits from end to start so earlier offsets stay valid.
    new_text = text
    for at, payload in sorted(edits, key=lambda e: e[0], reverse=True):
        new_text = new_text[:at] + payload + new_text[at:]

    return new_text, len(edits)


def _backup(path: Path) -> Path:
    stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    bak = path.with_suffix(path.suffix + f".bak-{stamp}")
    shutil.copy2(path, bak)
    return bak


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("files", nargs="+", type=Path, help="nginx site configs to patch")
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"upstream port to match (default {DEFAULT_PORT})",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="report what would change without modifying files; exit 1 if any block needs the include",
    )
    args = parser.parse_args(argv)

    needs_changes = 0
    changed = 0
    for path in args.files:
        if not path.is_file():
            print(f"WARN  not a regular file, skipping: {path}", file=sys.stderr)
            continue
        try:
            original = path.read_text()
        except OSError as e:
            print(f"ERROR cannot read {path}: {e}", file=sys.stderr)
            return 2

        try:
            new_text, n = _patch(original, args.port)
        except ValueError as e:
            print(f"ERROR cannot parse {path}: {e}", file=sys.stderr)
            return 2

        if n == 0:
            print(f"OK    {path} (no changes needed)")
            continue

        needs_changes += n
        if args.check:
            print(f"DIFF  {path} would inject include into {n} location block(s)")
            continue

        bak = _backup(path)
        path.write_text(new_text)
        changed += n
        print(f"PATCH {path} (+{n} block(s); backup: {bak.name})")

    if args.check and needs_changes > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
