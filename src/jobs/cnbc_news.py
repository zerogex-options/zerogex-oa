"""Scrape the day's top market headlines from CNBC for the bulletin tweet.

The bulletin auto-tweet feeds the day's news into the LLM alongside the
Live-Bulletin gamma structure and the symbol's price action, so the post
can open on what's actually driving the tape ("news of potential renewed
U.S.–Iran talks sent oil lower and sparked a sharp reversal").

We read CNBC's official RSS feeds rather than scraping the JavaScript-heavy
"stock market today live updates" page:

  * RSS is clean, machine-readable XML — no HTML/JS parsing, no bot-block.
  * The Markets / Top-News / Finance feeds carry the same headlines the
    live blog does, with a one-line summary each.

Design rules — same discipline as the rest of the bulletin pipeline:

  * **Never throws.**  Any network / parse error logs a WARNING and returns
    whatever resolved (``[]`` in the worst case).  A news outage must never
    take the tweet down — the LLM just writes without headlines.
  * **Config-gated.**  Disabled entirely by ``BULLETIN_TWEET_NEWS_ENABLED=0``.
    Feed URLs, item cap and timeout are all env-overridable.
  * **stdlib only.**  ``urllib`` + ``xml.etree`` — no new dependency.

Run manually:
    python -m src.jobs.cnbc_news            # prints the resolved headlines
"""

from __future__ import annotations

import html
import logging
import os
import re
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

logger = logging.getLogger("zerogex.cnbc_news")

# CNBC's official RSS feeds (https://www.cnbc.com/rss-feeds/).  The numeric
# id in the path selects the section.  These three cover the market tape;
# override the whole set with $BULLETIN_TWEET_NEWS_RSS_URLS if CNBC renumbers
# a feed or you want a different mix.
DEFAULT_FEEDS = (
    "https://www.cnbc.com/id/20910258/device/rss/rss.html",  # Markets
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",  # Top News
    "https://www.cnbc.com/id/10000664/device/rss/rss.html",  # Finance
)

DEFAULT_MAX_ITEMS = 6
DEFAULT_TIMEOUT_SECONDS = 12
# A real browser UA — CNBC's edge returns 403 to the stock urllib agent.
_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


@dataclass
class NewsItem:
    """One CNBC headline."""

    title: str
    summary: str = ""
    source: str = "CNBC"
    link: str | None = None
    published: str | None = None  # ISO-8601 when parseable, else the raw string
    published_ts: float | None = None  # epoch seconds, for sorting

    def to_dict(self) -> dict[str, Any]:
        return {
            "title": self.title,
            "summary": self.summary,
            "source": self.source,
            "link": self.link,
            "published": self.published,
        }


def _clean(text: str | None) -> str:
    """Decode HTML entities, strip tags, and collapse whitespace.

    CNBC RSS descriptions arrive with HTML markup and entities (often inside
    CDATA, so ``&amp;`` / ``&#39;`` reach us literally).  Unescape first so
    encoded tags become real tags we can strip, then drop tags + squeeze
    whitespace."""
    if not text:
        return ""
    return _WS_RE.sub(" ", _TAG_RE.sub("", html.unescape(text))).strip()


def _news_enabled() -> bool:
    return os.environ.get("BULLETIN_TWEET_NEWS_ENABLED", "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _feed_urls() -> list[str]:
    raw = os.environ.get("BULLETIN_TWEET_NEWS_RSS_URLS", "").strip()
    if raw:
        return [u.strip() for u in raw.split(",") if u.strip()]
    return list(DEFAULT_FEEDS)


def _max_items() -> int:
    raw = os.environ.get("BULLETIN_TWEET_NEWS_MAX_ITEMS", "").strip()
    if raw.isdigit() and int(raw) > 0:
        return int(raw)
    return DEFAULT_MAX_ITEMS


def _timeout_seconds() -> int:
    raw = os.environ.get("BULLETIN_TWEET_NEWS_TIMEOUT", "").strip()
    if raw.isdigit() and int(raw) > 0:
        return int(raw)
    return DEFAULT_TIMEOUT_SECONDS


def _fetch_feed(url: str, timeout: int) -> str | None:
    req = Request(
        url,
        headers={
            "User-Agent": _BROWSER_UA,
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        },
        method="GET",
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except (HTTPError, URLError) as exc:
        logger.warning("cnbc_news: feed fetch failed (%s): %s", url, exc)
        return None
    except Exception as exc:  # noqa: BLE001
        logger.warning("cnbc_news: unexpected feed error (%s): %s", url, exc)
        return None


def _parse_items(xml_text: str, source: str = "CNBC") -> list[NewsItem]:
    """Parse ``<item>`` entries out of an RSS document. Never raises."""
    items: list[NewsItem] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.warning("cnbc_news: RSS did not parse (%s)", exc)
        return items
    for item in root.iter("item"):
        title = _clean(item.findtext("title"))
        if not title:
            continue
        summary = _clean(item.findtext("description"))
        link = (item.findtext("link") or "").strip() or None
        pub_raw = (item.findtext("pubDate") or "").strip() or None
        published = pub_raw
        published_ts: float | None = None
        if pub_raw:
            try:
                dt = parsedate_to_datetime(pub_raw)
                if dt is not None:
                    published = dt.isoformat()
                    published_ts = dt.timestamp()
            except (TypeError, ValueError, OverflowError):
                pass
        items.append(
            NewsItem(
                title=title,
                summary=summary,
                source=source,
                link=link,
                published=published,
                published_ts=published_ts,
            )
        )
    return items


def fetch_headlines(
    max_items: int | None = None,
    feeds: list[str] | None = None,
    timeout: int | None = None,
) -> list[NewsItem]:
    """Return the day's top market headlines, most-recent first.

    Best-effort: returns ``[]`` when news is disabled or every feed fails.
    Deduplicates by title across feeds and caps the result at ``max_items``."""
    if not _news_enabled():
        logger.info("cnbc_news: disabled via BULLETIN_TWEET_NEWS_ENABLED — no headlines")
        return []

    urls = feeds if feeds is not None else _feed_urls()
    cap = max_items if max_items is not None else _max_items()
    tmo = timeout if timeout is not None else _timeout_seconds()

    collected: list[NewsItem] = []
    seen_titles: set[str] = set()
    for url in urls:
        xml_text = _fetch_feed(url, tmo)
        if not xml_text:
            continue
        for item in _parse_items(xml_text):
            key = item.title.lower()
            if key in seen_titles:
                continue
            seen_titles.add(key)
            collected.append(item)

    # Sort most-recent first when we have timestamps; items without one sink
    # to the bottom in feed order (stable sort keeps their relative order).
    collected.sort(key=lambda i: i.published_ts or 0.0, reverse=True)

    if not collected:
        logger.info("cnbc_news: no headlines resolved from %d feed(s)", len(urls))
    return collected[:cap]


def _main() -> int:
    logging.basicConfig(level=logging.INFO)
    heads = fetch_headlines()
    if not heads:
        print("(no headlines resolved)")
        return 0
    for i, h in enumerate(heads, 1):
        print(f"{i}. {h.title}")
        if h.summary:
            print(f"   {h.summary[:160]}")
        if h.published:
            print(f"   [{h.source} · {h.published}]")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
