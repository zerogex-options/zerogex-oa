#!/usr/bin/env python3
"""Inverse of ``nginx_inject_api_key.py``: strip the X-API-Key include
from every nginx ``location`` block that proxies to the ZeroGEX API.

Removes:
  1. The ``include /etc/nginx/conf.d/zerogex-api-key.conf*;`` line itself.
  2. The auto-injected comment immediately above it
     (``# Auto-injected by deploy: ...``) when present.

A timestamped ``.bak-YYYYMMDD-HHMMSS`` is written next to each file
that gets modified.  Files without the include are left untouched.

Usage:
    nginx_remove_api_key.py <site-config> [<site-config> ...]

Designed for use from ``deploy/steps/125.api_auth`` once the website
backend has its own per-user API key and no longer relies on nginx
injecting the static one.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import re
import shutil
import sys
from pathlib import Path

INCLUDE_NEEDLE = "zerogex-api-key.conf"
AUTO_COMMENT_NEEDLE = "Auto-injected by deploy"

# Match the include line (with optional trailing ``*`` and any whitespace).
INCLUDE_RE = re.compile(
    r"^[ \t]*include\s+/etc/nginx/conf\.d/zerogex-api-key\.conf\*?\s*;[ \t]*\n",
    re.MULTILINE,
)
AUTO_COMMENT_RE = re.compile(
    r"^[ \t]*#\s*Auto-injected by deploy:[^\n]*\n",
    re.MULTILINE,
)


def _strip(text: str) -> tuple[str, int]:
    """Return (new_text, num_includes_removed)."""
    new_text = text
    removed = 0

    # Walk include matches from end to start so earlier offsets stay valid
    # while we splice out optional preceding comment + the include line.
    matches = list(INCLUDE_RE.finditer(new_text))
    for m in reversed(matches):
        start, end = m.span()
        # If the line directly above is the auto-injected comment, take it
        # out as well so we don't leave dangling annotation.
        prev_line_end = start
        prev_line_start = new_text.rfind("\n", 0, prev_line_end - 1) + 1
        prev_line = new_text[prev_line_start:prev_line_end]
        if AUTO_COMMENT_NEEDLE in prev_line and prev_line.lstrip().startswith("#"):
            start = prev_line_start
        new_text = new_text[:start] + new_text[end:]
        removed += 1

    return new_text, removed


def _backup(path: Path) -> Path:
    stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    bak = path.with_suffix(path.suffix + f".bak-{stamp}")
    shutil.copy2(path, bak)
    return bak


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("files", nargs="+", type=Path, help="nginx site configs to clean")
    parser.add_argument(
        "--check",
        action="store_true",
        help="report what would change without modifying files; "
        "exit 1 if any include line is still present",
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

        if INCLUDE_NEEDLE not in original:
            print(f"OK    {path} (no include present)")
            continue

        new_text, n = _strip(original)
        if n == 0:
            # Needle present but our regex didn't match — manual edit needed.
            print(
                f"WARN  {path} contains '{INCLUDE_NEEDLE}' but no removable "
                f"include line was found; review manually",
                file=sys.stderr,
            )
            needs_changes += 1
            continue

        needs_changes += n
        if args.check:
            print(f"DIFF  {path} would remove {n} include line(s)")
            continue

        bak = _backup(path)
        path.write_text(new_text)
        changed += n
        print(f"STRIP {path} (-{n} block(s); backup: {bak.name})")

    if args.check and needs_changes > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
