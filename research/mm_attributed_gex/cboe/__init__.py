"""Cboe C1 Open-Close ingestion.

Deliberately the ONLY package in this experiment that knows what a Cboe file
looks like.  It converts vendor files into
:class:`~research.mm_attributed_gex.schema.ParticipantActivity` records and
stops there — it never computes gamma, never touches the database, and never
imports from :mod:`src`.

No Cboe sample file was available when this was written, so **nothing here
hard-codes a column name**.  The mapping lives in a declarative
:class:`~research.mm_attributed_gex.cboe.profiles.ColumnProfile`; ``inspect.py``
reads a real file's header and *proposes* a profile for a human to confirm.
See ``docs/design/market-maker-attributed-gex.md`` for the exact list of fields
that must be present.
"""

from research.mm_attributed_gex.cboe.profiles import (
    ColumnProfile,
    VolumeColumn,
    builtin_profiles,
    profile_from_dict,
    profile_to_dict,
)
from research.mm_attributed_gex.cboe.loader import (
    LoadResult,
    load_file,
    load_paths,
    records_from_rows,
)
from research.mm_attributed_gex.cboe.inspect import (
    HeaderProposal,
    propose_profile,
    read_header,
)

__all__ = [
    "ColumnProfile",
    "VolumeColumn",
    "builtin_profiles",
    "profile_from_dict",
    "profile_to_dict",
    "LoadResult",
    "load_file",
    "load_paths",
    "records_from_rows",
    "HeaderProposal",
    "propose_profile",
    "read_header",
]
