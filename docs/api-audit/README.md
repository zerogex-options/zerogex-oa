# API Audit — T0 Baseline

Baseline documents produced in Tier 0 of the API re-contract rollout. These
files capture the API surface **as of `release@bd80bdd`** (the branch point
for `claude/relaxed-noether-94r9og`). They are the ground truth every later
tier builds against.

| Document | Purpose |
|---|---|
| [endpoints.md](./endpoints.md) | Every HTTP route: method, path, tag, scope, response shape, freshness handling |
| [types.md](./types.md) | Every response Pydantic model: fields, timestamp conventions, unit conventions, null semantics |
| [route-drift.md](./route-drift.md) | Cross-check of current routes vs. previously-documented routes; catalogues any silent renames, removals, or drift |

## How to keep these current

These are **living baseline** documents, not one-shot audits.

- When adding, renaming, or removing a route, update `endpoints.md` in the same PR.
- When adding, renaming, or deprecating a response field, update `types.md` in the same PR.
- `route-drift.md` gets an entry every time a customer-visible route changes shape or path.

Once T4 lands (OpenAPI spec as source of truth + CI diff gate), most of this
becomes machine-generated. Until then, hand-maintained.
