"""Newsletter (Beehiiv) publications table read/write helpers.

Small mixin — three methods — for the ``newsletter_publications`` table
introduced with the Beehiiv Move-2 rollout. Kept off the main
``SignalsQueriesMixin`` because it's cron-only (the FastAPI surface
never reads or writes this table) and grouping the calls under one
module makes it obvious to a future reader which surface owns the writes.

See ``src/jobs/publish_beehiiv.py`` for the sole caller.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

logger = logging.getLogger(__name__)


class NewsletterQueriesMixin:
    """Read + write side of the ``newsletter_publications`` table.

    Inherits nothing; relies on ``self._acquire_connection`` from the
    ``DatabaseManager`` it's mixed into.
    """

    if TYPE_CHECKING:
        _acquire_connection: Callable[[], "AbstractAsyncContextManager[Any]"]

    async def get_newsletter_publication(
        self, cadence: str, publication_date: date
    ) -> Optional[Dict[str, Any]]:
        """Return the row for a (cadence, publication_date) pair, or
        None if none exists. Idempotency check for the publish cron."""
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT id, cadence, audience, publication_date,
                           beehiiv_post_id, beehiiv_url, subject_line,
                           body_html_sha256, status, error_message,
                           created_at, updated_at
                    FROM newsletter_publications
                    WHERE cadence = $1 AND publication_date = $2
                    """,
                    cadence, publication_date,
                )
                return dict(row) if row else None
        except Exception as exc:
            logger.warning(
                "get_newsletter_publication failed (%s, %s): %s",
                cadence, publication_date, exc,
            )
            return None

    async def upsert_newsletter_publication(
        self,
        *,
        cadence: str,
        audience: str,
        publication_date: date,
        subject_line: str,
        body_html_sha256: str,
        status: str,
        beehiiv_post_id: Optional[str] = None,
        beehiiv_url: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Insert or update the (cadence, publication_date) row.

        The unique constraint on (cadence, publication_date) makes this an
        UPSERT: the first run inserts a ``status='pending'`` row, subsequent
        transitions (published / failed) update it in place. Callers that
        succeed pass ``status='published'`` + ``beehiiv_post_id`` +
        ``beehiiv_url``; failures pass ``status='failed'`` + ``error_message``
        so the operator has both the intended and the actual state.
        """
        try:
            async with self._acquire_connection() as conn:
                row = await conn.fetchrow(
                    """
                    INSERT INTO newsletter_publications (
                        cadence, audience, publication_date,
                        beehiiv_post_id, beehiiv_url, subject_line,
                        body_html_sha256, status, error_message
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (cadence, publication_date) DO UPDATE
                    SET audience         = EXCLUDED.audience,
                        beehiiv_post_id  = COALESCE(EXCLUDED.beehiiv_post_id, newsletter_publications.beehiiv_post_id),
                        beehiiv_url      = COALESCE(EXCLUDED.beehiiv_url, newsletter_publications.beehiiv_url),
                        subject_line     = EXCLUDED.subject_line,
                        body_html_sha256 = EXCLUDED.body_html_sha256,
                        status           = EXCLUDED.status,
                        error_message    = EXCLUDED.error_message
                    RETURNING id, cadence, audience, publication_date,
                              beehiiv_post_id, beehiiv_url, subject_line,
                              body_html_sha256, status, error_message,
                              created_at, updated_at
                    """,
                    cadence,
                    audience,
                    publication_date,
                    beehiiv_post_id,
                    beehiiv_url,
                    subject_line,
                    body_html_sha256,
                    status,
                    error_message,
                )
                return dict(row) if row else None
        except Exception as exc:
            logger.warning(
                "upsert_newsletter_publication failed (%s, %s): %s",
                cadence, publication_date, exc,
            )
            return None
