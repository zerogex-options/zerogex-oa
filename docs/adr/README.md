# Architecture Decision Records (ADRs)

Durable records of the architectural decisions we make and, more
importantly, *why* we made them. ADRs are the memory of the design
process — they let the person who reads the code in eighteen months
understand what the person who wrote it was optimizing for, and what
they explicitly chose not to do.

## Format

Every ADR follows the same structure:

- **Status** — one of `Proposed`, `Accepted`, `Superseded by ADR-N`, `Deprecated`.
- **Context** — what problem is being decided, what forcing functions
  led us here, what constraints apply.
- **Decision** — the choice made, stated crisply.
- **Rationale** — why this choice, why not the alternatives, with the
  alternatives named and honestly evaluated.
- **Consequences** — what changes (positive and negative), what becomes
  possible, what becomes constrained.
- **References** — links to prior art, related ADRs, external specs,
  customer conversations that drove the decision.

An ADR is a decision, not a design doc. Design specifics live inside the
decision only when they *are* the decision (e.g. ADR-0002's envelope
schema is the decision). Otherwise the ADR points to a spec or a code
location.

## Numbering

Zero-padded four-digit sequence. Numbers are **immutable** — a superseded
ADR keeps its number and gains a `Superseded by ADR-N` status; the
successor gets a new number. This preserves history.

## Lifecycle

1. Open a PR with a `Proposed` ADR.
2. Discuss on the PR. When consensus lands, edit the ADR to `Accepted`
   in the same PR.
3. Land it.
4. If a later decision changes course, that decision gets its own ADR
   (never edit an `Accepted` one). The old ADR's status becomes
   `Superseded by ADR-N`.

**Never delete an ADR.** The record of what we tried and moved away from
is often more valuable than the current answer.

## Index

| # | Title | Status |
|---|---|---|
| [0001](./0001-recontract-api-for-freshness-and-versioning.md) | Re-contract the API for freshness, semantics, and versioning | Accepted |
| [0002](./0002-canonical-response-envelope.md) | Canonical response envelope with freshness, status, and per-field reason codes | Accepted |
| [0003](./0003-api-versioning-strategy.md) | API versioning strategy: hard-cut to `/v1/`, OpenAPI as source of truth | Accepted |
| [0004](./0004-deprecation-policy-and-customer-communication.md) | Deprecation policy and customer communication protocol | Accepted |
