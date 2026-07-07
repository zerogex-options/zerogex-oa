"""Approve (or discard) a staged bulletin tweet draft.

Companion to :mod:`src.jobs.bulletin_tweet`.  The tweet job's ``--stage``
mode writes ``state=pending`` drafts to
``$BULLETIN_TWEET_ARTIFACT_DIR/<mode>/<date>/`` for human review.  This
script is the "approve" side of the loop — the operator reviews the
staged draft (via SSH, email, notification hook, whatever) and then
runs:

    python -m src.jobs.bulletin_approve --mode close                # POST it
    python -m src.jobs.bulletin_approve --mode close --discard      # drop it
    python -m src.jobs.bulletin_approve --mode close --long/--short # override

By default reads today's draft in the given mode.  Overriding with
``--date`` allows approving yesterday's late-night stage in the morning.

Design rules mirror :mod:`bulletin_tweet`:

  * Never throws.  Failure to POST logs a warning and returns non-zero
    without moving the draft into ``posted`` state, so the operator
    can retry.
  * Idempotent: if the manifest already says ``state=posted`` the
    script exits early with the previous tweet id.  You can't
    accidentally double-post.
  * Copy-paste fallback: if ``X_BOT_BEARER_TOKEN`` isn't configured
    (X developer application still pending), ``--print`` dumps the
    tweet text + media paths to stdout so the operator can paste into
    the X web UI manually.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

from src.jobs.bulletin_tweet import (
    MediaArtifacts,
    TweetBody,
    post_bulletin,
    resolve_artifact_dir,
)

logger = logging.getLogger("zerogex.bulletin_approve")
ET = ZoneInfo("America/New_York")

MODES = ("premarket", "midday", "close")


def _today_et() -> date:
    return datetime.now(tz=ET).date()


def _load_manifest(artifact_dir: Path) -> dict[str, Any] | None:
    manifest_path = artifact_dir / "manifest.json"
    if not manifest_path.exists():
        return None
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_approve: failed to parse %s (%s)", manifest_path, exc)
        return None


def _load_tweet_body(artifact_dir: Path, manifest: dict[str, Any]) -> TweetBody | None:
    text_path = artifact_dir / "tweet_text.md"
    fallback_path = artifact_dir / "tweet_text_fallback.md"
    if not text_path.exists() or not fallback_path.exists():
        logger.warning(
            "bulletin_approve: missing tweet_text files in %s — nothing to post",
            artifact_dir,
        )
        return None
    return TweetBody(
        text=text_path.read_text(encoding="utf-8").rstrip("\n"),
        fallback=fallback_path.read_text(encoding="utf-8").rstrip("\n"),
        lead_symbol=manifest.get("lead_symbol", ""),
        symbols_present=list(manifest.get("symbols_present") or []),
    )


def _load_media(manifest: dict[str, Any]) -> MediaArtifacts:
    media = MediaArtifacts()
    png = (manifest.get("media") or {}).get("png")
    clip = (manifest.get("media") or {}).get("clip")
    if png and Path(png).exists():
        media.png_path = Path(png)
    if clip and Path(clip).exists():
        media.clip_path = Path(clip)
    return media


def _print_for_manual_posting(
    mode: str, artifact_dir: Path, tweet: TweetBody, media: MediaArtifacts,
) -> None:
    """When X credentials aren't configured, dump the draft in a format
    the operator can copy-paste into X's web compose UI.

    Especially useful while the X developer application is pending: the
    tweet pipeline still generates real content; the operator posts it
    manually until the API side is unblocked."""
    print(f"\n{'=' * 64}")
    print(f"MANUAL POSTING MODE — no X_BOT_BEARER_TOKEN configured")
    print(f"{'=' * 64}")
    print(f"Mode:         {mode}")
    print(f"Artifacts:    {artifact_dir}")
    if media.png_path:
        print(f"PNG:          {media.png_path}")
    if media.clip_path:
        print(f"Clip:         {media.clip_path}")
    print(f"Text length:  {len(tweet.text)} (needs X Premium)")
    print(f"Fallback:     {len(tweet.fallback)} chars (fits classic 280)")
    print(f"{'-' * 64}")
    print(tweet.text)
    print(f"{'=' * 64}\n")


def _update_manifest_state(
    artifact_dir: Path,
    state: str,
    posted_id: str | None = None,
) -> None:
    """Rewrite manifest.json with the new state, preserving other fields.

    Called after a successful POST (state=posted) or a --discard
    (state=discarded) so the operator can distinguish drafts by the
    final field alone, without re-running the tweet job."""
    manifest_path = artifact_dir / "manifest.json"
    manifest = _load_manifest(artifact_dir) or {}
    manifest["state"] = state
    if posted_id is not None:
        manifest["posted_id"] = posted_id
    manifest["approved_ts"] = datetime.now(tz=ET).isoformat()
    manifest_path.write_text(
        json.dumps(manifest, indent=2, default=str) + "\n", encoding="utf-8",
    )


def _run(args: argparse.Namespace) -> int:
    day = date.fromisoformat(args.date) if args.date else _today_et()
    artifact_dir = resolve_artifact_dir(args.artifact_dir, args.mode, day)

    manifest = _load_manifest(artifact_dir)
    if manifest is None:
        logger.warning(
            "bulletin_approve: no manifest at %s — nothing to approve.",
            artifact_dir,
        )
        return 1

    state = manifest.get("state") or "unknown"
    if state == "posted":
        logger.info(
            "bulletin_approve[%s]: draft already posted (id=%s) — nothing to do.",
            args.mode, manifest.get("posted_id"),
        )
        return 0
    if state == "discarded":
        logger.info(
            "bulletin_approve[%s]: draft was discarded — nothing to do.", args.mode,
        )
        return 0
    if state not in ("pending", "dry_run"):
        logger.warning(
            "bulletin_approve[%s]: manifest state=%r is not eligible for approval.",
            args.mode, state,
        )
        return 1

    tweet = _load_tweet_body(artifact_dir, manifest)
    if tweet is None:
        return 1
    media = _load_media(manifest)

    if args.discard:
        _update_manifest_state(artifact_dir, state="discarded")
        logger.info(
            "bulletin_approve[%s]: draft discarded (was %s).", args.mode, state,
        )
        return 0

    bearer = os.environ.get("X_BOT_BEARER_TOKEN", "").strip()
    if not bearer or args.print_only:
        # Copy-paste path.  Especially useful while the X developer
        # application is still pending review.
        _print_for_manual_posting(args.mode, artifact_dir, tweet, media)
        if args.mark_posted:
            _update_manifest_state(
                artifact_dir, state="posted", posted_id="manual",
            )
            logger.info(
                "bulletin_approve[%s]: marked manifest state=posted (manual).",
                args.mode,
            )
        return 0

    post_result = post_bulletin(
        tweet=tweet,
        media=media,
        bearer=bearer,
        long=args.long,
        mode_label=args.mode,
    )
    if not post_result:
        logger.warning(
            "bulletin_approve[%s]: post failed — draft remains pending, retry with the same command.",
            args.mode,
        )
        return 1

    _update_manifest_state(
        artifact_dir, state="posted", posted_id=post_result.get("id"),
    )
    logger.info(
        "bulletin_approve[%s]: posted tweet id=%s (artifacts=%s)",
        args.mode, post_result.get("id"), artifact_dir,
    )
    return 0


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=MODES,
        required=True,
        help="Which of the three daily fires to approve.",
    )
    parser.add_argument(
        "--date",
        help="Target date (YYYY-MM-DD). Default: today ET.",
    )
    parser.add_argument(
        "--discard",
        action="store_true",
        help="Reject the draft — mark discarded, do not post.",
    )
    parser.add_argument(
        "--print", "-p",
        dest="print_only",
        action="store_true",
        help=(
            "Force the copy-paste path even when X credentials are configured.  "
            "Useful for eyeballing what would be posted."
        ),
    )
    parser.add_argument(
        "--mark-posted",
        action="store_true",
        help=(
            "When used with --print (or when X isn't configured), mark the "
            "manifest as posted=manual after printing.  Use this once you've "
            "pasted the draft into the X web UI so it doesn't show up in "
            "pending listings."
        ),
    )
    parser.add_argument(
        "--long",
        action="store_true",
        default=True,
        help="Post the full multi-paragraph body (default; X Premium required).",
    )
    parser.add_argument(
        "--short",
        dest="long",
        action="store_false",
        help="Force the 280-char fallback body.",
    )
    parser.add_argument(
        "--artifact-dir",
        default=None,
        help="Override the base artifact directory to scan.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _parse_args(argv)
    return _run(args)


if __name__ == "__main__":
    sys.exit(main())
