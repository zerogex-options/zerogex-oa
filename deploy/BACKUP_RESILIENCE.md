# Backup & Disaster Recovery Checklist

Operator checklist for the AWS-side settings that make ZeroGEX data
recoverable. These are configured in the AWS console / CLI, not in code,
so they are easy to forget — this file is the source of truth for what
"done" looks like. Pair it with the in-repo backup tooling:

- **Auth DB (web):** `make backup-auth` in `zerogex-web` — see that repo's `deploy/README.md` ("Auth Database Backups").
- **RDS logical dump (oa):** `/data/backups` is provisioned by `deploy/steps/015.data_volume` for `pg_dump` output (target not yet wired up — see below).
- **Monitoring JSON (web):** `make backup-monitoring`.

The `deploy/README.md` "AWS RDS Best Practices" section lists several of
these as "consider for production"; this checklist turns them into
concrete, checkable items with the reasoning.

## Data inventory

| Store | Location | Contains | Reconstructible? | Backup owner |
|---|---|---|---|---|
| **Auth DB (SQLite)** | web instance volume, `AUTH_DB_PATH` (`/var/lib/zerogex/auth.db`) | users, password hashes, sessions, OAuth identities, password-reset tokens, audit log, user→tier mapping | **No** — only copy of account data | `make backup-auth` + S3 |
| **RDS PostgreSQL 17** | AWS RDS (TimescaleDB ext.) | option_chains, gex_summary, signals, ingestion history | Partially — re-ingestable from the market-data vendor, but recent history + derived signals are costly/impossible to rebuild | RDS automated backups + logical dump |
| **Monitoring/signups JSON** | web instance, `frontend/data/*.json` | usage counters, daily signup totals | Mostly | `make backup-monitoring` |
| **Stripe** | Stripe (external) | billing source of truth | N/A — Stripe is durable | Stripe |

Billing truth lives in Stripe, but the **mapping** from your users to
Stripe customers lives only in the auth DB. Lose the auth DB and you
cannot cleanly reconcile subscriptions — which is why it is Tier 1.

## RPO / RTO targets

Set these explicitly so backup frequency and HA choices have a yardstick.
Suggested starting points (tune to the business):

| Store | RPO (max data loss) | RTO (max downtime) | How it's met |
|---|---|---|---|
| Auth DB | 1 hour | 1 hour | hourly `make backup-auth` → S3; restore is a `gunzip` + restart |
| RDS | 5 minutes | a few hours | automated backups + PITR (transaction logs); Multi-AZ for failover |
| Monitoring JSON | 1 day | best-effort | daily `make backup-monitoring` |

## Checklist

### RDS PostgreSQL

- [ ] **Automated backups enabled** with retention **≥ 30 days** (default is 7). Enables PITR over that window.
- [ ] **Point-in-time recovery (PITR)** confirmed — automatic once automated backups + transaction-log archiving are on. Verify the "Latest restorable time" tracks within ~5 min in the console.
- [ ] **Multi-AZ deployment** for automatic failover (HA, not backup — does not protect against logical corruption or bad deploys; still need backups).
- [ ] **Deletion protection** ON, and **final snapshot** required on delete, so an accidental teardown can't vaporize the DB.
- [ ] **Independent logical dump.** RDS snapshots are locked to RDS/AWS. Run a periodic `pg_dump` (full data, not the `--schema-only` `make schema-backup`) into `/data/backups`, gzip, and upload to S3 — a portable, account-independent copy you can restore anywhere. Schedule alongside the 02:00 ET retention cron.
- [ ] **Cross-region snapshot copy** (and/or cross-region automated backup replication) so a single-region outage doesn't take the backups with it.
- [ ] **Snapshots/backups in a separate AWS account** (or at least a separate IAM boundary) so a compromised prod credential cannot delete both prod and its backups.
- [ ] **Low free-storage alarm** (< 10 GB) — the May-26 postmortem noted `/dev/root` at 90%; a full disk both breaks ingestion and can wedge backups.

### Auth DB backups (S3 destination for `make backup-auth`)

- [ ] **Dedicated S3 prefix/bucket**, **private** (Block Public Access ON), **default SSE** (SSE-S3 or SSE-KMS).
- [ ] **Versioning ON** — protects against an overwrite/delete clobbering good backups.
- [ ] **Object Lock (compliance or governance mode)** or a deny-delete bucket policy — ransomware / rogue-credential resilience; backups can't be silently wiped.
- [ ] **Lifecycle policy** — transition to cheaper storage after N days, expire well past your retention window.
- [ ] **Either** `BACKUP_GPG_RECIPIENT` set on the cron **or** SSE-KMS with a tight key policy — the archive holds password hashes + PII; do not store it unencrypted in a loosely-scoped bucket.
- [ ] **Cross-region replication** of this prefix.
- [ ] IAM for the EC2 instance role scoped to **`s3:PutObject` only** on this prefix (no delete/get), so a compromised web box can write backups but not read or destroy history.

### EBS / instance volumes (web box where auth.db lives)

- [ ] **AWS Backup plan or DLM policy** taking daily EBS snapshots of the web instance's volume(s) — belt-and-suspenders for the whole box, complements the per-file `make backup-auth`.
- [ ] Snapshot retention ≥ 30 days; copy to a second region.

### Restore drills (the part everyone skips)

An untested backup is not a backup. The `deploy/README.md` already says
"Test restore procedures regularly" — make it a calendar item.

- [ ] **Quarterly:** restore the latest auth-DB archive into a scratch path, run `PRAGMA integrity_check`, and confirm a known user/email is present.
- [ ] **Quarterly:** restore an RDS snapshot (or the logical dump) into a throwaway instance and run a few representative queries.
- [ ] **Document the measured RTO** from each drill and compare to the targets above; if a restore takes longer than the RTO, fix the procedure, not the spreadsheet.
- [ ] Keep the runbook steps next to `deploy/RUNBOOK.md` so they're found under pressure.

## 3-2-1, mapped to this stack

- **3 copies:** live store + automated AWS backup (RDS snapshots / EBS snapshots) + logical export (pg_dump / `make backup-auth` archive).
- **2 media/services:** RDS or instance volume **and** S3.
- **1 offsite:** cross-region (and ideally cross-account) copy of the S3 archives and RDS snapshots.

## Notes from the May-26 postmortem (related)

These aren't backups, but they're the same "resilience" bucket and were
flagged as open action items:

- Size up RDS (`shared_buffers` 444 MB vs ~23 GB working set) — a starved
  DB is more likely to wedge under load; suggested `db.m5.large`.
- Investigate disk performance (4.5 ms/page, ~10× slower than gp3 should
  be) — slow IO lengthens both query times and restore times.
- `/dev/root` at 90% full — a full disk can silently break the very
  backup jobs you're relying on.
