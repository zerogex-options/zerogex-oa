"""Tests for src.jobs.cnbc_news — the CNBC RSS headline scraper feeding the
bulletin tweet's LLM.

The parser is exercised against sample RSS fixtures (no network); the
fetch orchestration is exercised by stubbing ``_fetch_feed``.  Contract:
never raises, config-gated, dedups by title, sorts most-recent first."""

from __future__ import annotations

import pytest

from src.jobs import cnbc_news

_SAMPLE_FEED_A = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
<title>CNBC Markets</title>
<item>
  <title>Stocks fall as oil spikes on Middle East tension</title>
  <description><![CDATA[<p>The S&amp;P 500 slid as crude jumped.</p>]]></description>
  <link>https://www.cnbc.com/2026/07/24/a.html</link>
  <pubDate>Fri, 24 Jul 2026 13:30:00 GMT</pubDate>
</item>
<item>
  <title>Fed official signals patience on rate cuts</title>
  <description>Comments from a policymaker moved yields.</description>
  <link>https://www.cnbc.com/2026/07/24/b.html</link>
  <pubDate>Fri, 24 Jul 2026 14:00:00 GMT</pubDate>
</item>
</channel></rss>
"""

# Feed B repeats the Fed headline (dedup) and adds a newer one.
_SAMPLE_FEED_B = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
<item>
  <title>Fed official signals patience on rate cuts</title>
  <description>Duplicate of feed A.</description>
  <link>https://www.cnbc.com/2026/07/24/b.html</link>
  <pubDate>Fri, 24 Jul 2026 14:00:00 GMT</pubDate>
</item>
<item>
  <title>Oil eases after report of renewed talks</title>
  <description>Crude pared gains late in the session.</description>
  <link>https://www.cnbc.com/2026/07/24/c.html</link>
  <pubDate>Fri, 24 Jul 2026 15:15:00 GMT</pubDate>
</item>
</channel></rss>
"""


def test_clean_strips_tags_and_whitespace():
    assert cnbc_news._clean("<p>Hello   world</p>") == "Hello world"
    assert cnbc_news._clean(None) == ""
    assert cnbc_news._clean("  a\n\n b ") == "a b"


def test_parse_items_extracts_fields():
    items = cnbc_news._parse_items(_SAMPLE_FEED_A)
    assert len(items) == 2
    first = items[0]
    assert first.title == "Stocks fall as oil spikes on Middle East tension"
    # HTML tags + entities collapsed out of the summary.
    assert first.summary == "The S&P 500 slid as crude jumped."
    assert first.link == "https://www.cnbc.com/2026/07/24/a.html"
    # pubDate parsed to ISO + epoch.
    assert first.published.startswith("2026-07-24T13:30:00")
    assert first.published_ts is not None


def test_parse_items_bad_xml_returns_empty():
    assert cnbc_news._parse_items("<not xml") == []


def test_fetch_headlines_dedups_and_sorts_recent_first(monkeypatch):
    monkeypatch.setenv("BULLETIN_TWEET_NEWS_ENABLED", "1")
    feeds = {"A": _SAMPLE_FEED_A, "B": _SAMPLE_FEED_B}
    calls = iter(["A", "B"])

    def _fake_fetch(url, timeout):
        return feeds[next(calls)]

    monkeypatch.setattr(cnbc_news, "_fetch_feed", _fake_fetch)
    heads = cnbc_news.fetch_headlines(feeds=["urlA", "urlB"], max_items=10)

    titles = [h.title for h in heads]
    # Duplicate "Fed official…" appears once.
    assert titles.count("Fed official signals patience on rate cuts") == 1
    # Sorted most-recent first: 15:15 > 14:00 > 13:30.
    assert titles[0] == "Oil eases after report of renewed talks"
    assert titles[-1] == "Stocks fall as oil spikes on Middle East tension"
    assert len(heads) == 3


def test_fetch_headlines_respects_max_items(monkeypatch):
    monkeypatch.setenv("BULLETIN_TWEET_NEWS_ENABLED", "1")
    monkeypatch.setattr(cnbc_news, "_fetch_feed", lambda url, timeout: _SAMPLE_FEED_A)
    heads = cnbc_news.fetch_headlines(feeds=["u"], max_items=1)
    assert len(heads) == 1


def test_fetch_headlines_disabled_returns_empty(monkeypatch):
    monkeypatch.setenv("BULLETIN_TWEET_NEWS_ENABLED", "0")
    # Should never even call _fetch_feed when disabled.
    monkeypatch.setattr(
        cnbc_news,
        "_fetch_feed",
        lambda url, timeout: (_ for _ in ()).throw(AssertionError("fetched while disabled")),
    )
    assert cnbc_news.fetch_headlines(feeds=["u"]) == []


def test_fetch_headlines_survives_feed_failure(monkeypatch):
    monkeypatch.setenv("BULLETIN_TWEET_NEWS_ENABLED", "1")
    monkeypatch.setattr(cnbc_news, "_fetch_feed", lambda url, timeout: None)
    assert cnbc_news.fetch_headlines(feeds=["u1", "u2"]) == []


def test_news_item_to_dict_shape():
    item = cnbc_news.NewsItem(title="t", summary="s", source="CNBC", link="l", published="p")
    d = item.to_dict()
    assert d == {"title": "t", "summary": "s", "source": "CNBC", "link": "l", "published": "p"}
