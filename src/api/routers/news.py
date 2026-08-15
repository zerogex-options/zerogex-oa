"""Public read endpoint exposing the day's CNBC market headlines.

These are the *exact same* headlines the automated Live-Bulletin X (Twitter)
post is built from: the endpoint calls :func:`src.jobs.cnbc_news.fetch_headlines`
— the same function the bulletin tweet pipeline uses — so the website's
"Top Headlines" surface and the auto-tweet always see one source of truth.
If ops re-points the feed set via ``$BULLETIN_TWEET_NEWS_RSS_URLS`` (or caps
it with ``$BULLETIN_TWEET_NEWS_MAX_ITEMS``), both follow automatically.

Endpoint (prefix ``/api/news``):

* ``GET /headlines`` — the most-recent, deduped CNBC headlines (title +
  one-line summary + link + published timestamp), newest first.

Design notes:

* **Not premium.** Unlike the GEX/flow/signals routers, this carries no
  ``require_scopes`` gate — market headlines are broadly redistributable.
  It still rides the app-wide :func:`api_key_auth` dependency, so the
  colocated website BFF reaches it with its normal service token.
* **Never 500s on a news outage.** ``fetch_headlines`` is best-effort and
  returns ``[]`` on any network/parse failure; the handler wraps it so an
  upstream CNBC hiccup degrades to an empty list, never an error the badge
  or dashboard widget would have to render around.
* **No DB.** The scraper is stdlib-only and stateless, so there is no
  database dependency to initialize.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query
from starlette.concurrency import run_in_threadpool

from src.jobs import cnbc_news

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/news", tags=["News"])

# Hard ceiling on the caller-supplied ``limit`` so a bad query param can't
# ask the scraper to hand back an unbounded list. The bulletin default
# (``BULLETIN_TWEET_NEWS_MAX_ITEMS``, 6) still applies when no limit is given.
_MAX_LIMIT = 30


@router.get("/headlines")
async def headlines(
    limit: Optional[int] = Query(
        default=None,
        ge=1,
        le=_MAX_LIMIT,
        description=(
            "Maximum number of headlines to return. Defaults to the bulletin "
            "tweet's configured cap ($BULLETIN_TWEET_NEWS_MAX_ITEMS, 6)."
        ),
    ),
) -> Dict[str, Any]:
    """Return the CNBC headlines the automated X post is built from.

    Best-effort: an empty ``headlines`` list means news is disabled
    (``BULLETIN_TWEET_NEWS_ENABLED=0``) or every upstream feed failed — the
    website renders a clean empty state rather than an error.
    """
    try:
        # fetch_headlines() does blocking urllib I/O against CNBC's RSS
        # feeds; hop to a worker thread so it never stalls the event loop.
        items = await run_in_threadpool(cnbc_news.fetch_headlines, limit)
    except Exception as exc:  # noqa: BLE001 — a news outage must not 500
        logger.warning("news.headlines: fetch failed (%s)", exc, exc_info=True)
        items = []

    return {
        "source": "cnbc",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
        "headlines": [item.to_dict() for item in items],
    }
