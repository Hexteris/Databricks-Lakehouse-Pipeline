# Q&A bank

Say each answer out loud. Short first, detail only if asked.

## Architecture
**Why three layers?** Bronze is a faithful copy, so nothing is ever lost and everything can be rebuilt. Silver is trusted, cleaned data. Gold is shaped for questions. Each layer has one job, so a bug in one is fixed without touching the others.

**Can you skip silver?** For a quick report, yes, but then every consumer repeats the cleaning and DQ, and they'll disagree. Silver is where "one version of the truth" lives.

**Why is landing a volume, not a table?** Files arrive in their own formats. The volume keeps them exactly as received, governed by Unity Catalog, and the reset replays from it.

## Bronze and Auto Loader
**How do you avoid loading a file twice?** The Auto Loader checkpoint records every file processed. A re-run only picks up new files. The Job re-run loaded 0 rows.

**What does `availableNow` do?** Processes everything new, then stops. Batch-style cost, with streaming's file tracking.

**Why is bronze all strings?** So nothing is lost or mis-converted (leading zeros, odd dates). Typing is a silver decision.

**What happens with a new column?** With `mergeSchema` on the writer, the stream stops once, records the new schema and succeeds on restart. The Job retry handles that. Session-wide auto-merge is blocked on Free Edition, so I opt in per table.

**Why not COPY INTO?** It also loads files idempotently, and suits simple SQL loads. Auto Loader scales better to many files and handles schema evolution and rescued data.

## Silver and data quality
**How do the DQ rules work?** Each rule is a SQL condition in `ops.dq_rule`. Silver turns them into `CASE WHEN rule THEN NULL ELSE rule_id` and collects failures per row. REJECT goes to quarantine; WARN is kept and counted.

**Why does NULL count as a failure?** Otherwise a missing value passes every check. `coalesce(rule, false)`.

**Why dedup before MERGE?** MERGE fails if two source rows match the same target row. And rows with no key are kept, not deduplicated, so each one is quarantined rather than silently lost.

**Why check referrals against silver clients, not bronze?** A referral is only valid if its client is valid. That gives cascading quarantine and, later, automatic release.

**How do you know nothing was lost?** Reconciliation: silver + open quarantine = distinct bronze. Fact rows = silver referrals, with zero `-1` keys.

**Why no watermark?** The checkpoint tracks files, and silver re-reads bronze and MERGEs with `row_hash`. At this size that's idempotent and cheap. At scale I'd stream from bronze to silver.

**What if batch 2 and 3 arrive before the pipeline runs?** Silver keeps only the newest version, so an intermediate SCD2 version would be lost. Fix: process per batch (the Job), or build SCD2 from bronze history, which keeps every version.

**How do you handle deletes?** I don't in this demo. In production: a soft-delete flag or a CDC feed from the source, closing the SCD2 row rather than removing it.

## PII and security
**Hash vs encrypt vs mask vs tokenise?** Hash: one-way, same input gives the same output, so it's a join key. Encrypt: reversible with the key. Mask: changes what a user sees, not what's stored. Tokenise: replace with a token, with the real value held in a separate vault.

**Why salt the NRIC hash?** NRICs have a known format and a small number of possible values, so an unsalted hash can be reversed by hashing every possible NRIC. The salt is a secret.

**Why can't you join on encrypted columns?** AES-GCM uses a random IV, so the same value encrypts differently every time. That's why the join key is the hash.

**Where are the keys?** In a Databricks secret scope, read with `secret()`. Never in code or Git, and redacted in output. In production, backed by a cloud key vault.

**Key rotation?** Encryption key: decrypt with the old key, re-encrypt with the new one. Salt rotation changes every hash, so it needs a remapping. That's why the salt is rotated rarely.

**Why do masks matter if data is already encrypted?** Different job. Encryption protects storage, masks control what each person sees. Masks are on the column, so no view can bypass them.

**PDPA?** At a high level: collect and use personal data only for its purpose, protect it, limit access. Synthetic data here; in production, classification drives which columns are hashed, encrypted and masked.

## Modelling and SCD
**What's the fact grain?** One row per referral.

**Why surrogate keys?** One client can have several SCD2 versions; the fact must point at exactly one. Surrogates also decouple gold from source ids.

**SCD types?** 0 never changes, 1 overwrite, 2 new row per change with validity dates, 3 previous value in a column, 4 history in a separate table, 6 a mix of 1, 2 and 3.

**Why Type 2 for area and household, Type 1 for phone?** Circumstances matter historically (where was the client when referred?). An old phone number has no business value.

**How does your SCD2 MERGE work?** Changed clients appear twice in the source: once to close the current row, once with a NULL key so it's inserted as the new version. One MERGE, one transaction, so a client never has zero or two current rows.

**What's `scd_hash` vs `row_hash`?** `scd_hash` covers the Type 2 columns: different means new version. `row_hash` covers everything: different with the same `scd_hash` means a Type 1 overwrite.

**Point-in-time join?** Referral date ≥ `effective_from` and < `effective_to`. Location comes from that version too.

**Star vs snowflake vs wide?** Star: denormalised dimensions, simple joins. Snowflake: dimensions normalised into sub-tables (e.g. region), less redundancy, more joins. Wide: everything pre-joined, fastest to query, must be rebuilt when dimensions change. I kept location as one table at 55 rows; a snowflake would split region out.

**Late-arriving dimension?** I quarantine the referral (REF_003) and release it when the client arrives. The alternative is an inferred member: a placeholder client row, updated later.

**Is time travel a replacement for SCD2?** No. Time travel tracks table versions and is limited by `VACUUM` retention. SCD2 is permanent, queryable business history.

## Remediation
**Why replay through bronze?** The fix goes through the same rules, nothing is edited by hand, bronze stays append-only for audit, and everything can be rebuilt.

**What if the correction is wrong?** It's quarantined again as a new row. The old one stays REPLAYED, never RESOLVED without proof.

**Why do 4 rows stay open?** They have no usable business key, so a correction can't target them. They need a fix in the source system, and they're visible rather than dropped.

**How do you roll back a bad batch?** If nothing good has loaded since: `RESTORE TABLE ... TO VERSION AS OF`. Otherwise delete by `_run_id`, which only removes that load.

## Delta and Spark
**What does Delta add?** A transaction log: ACID writes, MERGE, schema enforcement, history, time travel, RESTORE.

**Lazy evaluation?** Transformations build a plan; actions (count, display, write) run it, so Catalyst can optimise the whole plan first.

**Why avoid `collect()`?** It pulls all data to the driver. I only use it on tiny results (rules, the replayed keys).

**`OPTIMIZE`, `VACUUM`, liquid clustering?** OPTIMIZE compacts small files. VACUUM deletes files no longer referenced, which limits time travel. Liquid clustering organises data by chosen columns for faster filtering.

## Orchestration and production
**How is it orchestrated?** One Job, explicit dependencies, parallel where possible, retry on bronze, `batch_label` parameter. Setup is excluded because it can reset.

**What did the Job re-run prove?** End-to-end idempotency, that the parameter reaches every notebook, that MERGE works on the masked table, and it caught up a step I had skipped by hand.

**What would you change for production?** Asset Bundles with CI/CD, file-arrival or scheduled triggers, alerts on FAILED runs and DQ spikes, groups and service principals only, streaming silver and an incremental fact at scale.

## Frameworks
**Lake vs warehouse vs lakehouse?** Lake: cheap files, no transactions. Warehouse: reliable SQL, closed and costly. Lakehouse: warehouse features (ACID, governance) on open lake files, one copy for BI and ML.

**What is data mesh?** An organisational model: domain ownership, data as a product, a self-serve platform, federated governance.

**When is mesh a bad idea?** Few data teams, low maturity, or mostly cross-domain questions. A central platform is simpler.

## Your choices
**Why synthetic data?** I needed realistic PII and controlled defects, and must never use real personal data. The one real dataset (census) is public and gave me real-world messiness.

**Why generic programme names?** Invented volumes and approval rates shouldn't be attached to real MSF schemes.

**What would you do with more time?** A dashboard on `dq_result` and quarantine, row filters per office, deletes via CDC, and tests in CI.

**What was hardest?** SCD2 in one MERGE, and making sure counts reconcile at every layer.

## Questions to ask MSF
- What source systems feed the platform today, and what does ingestion look like?
- How is sensitive citizen data classified and governed?
- Is the team moving towards a data mesh or a central platform?
- How do you measure data quality and handle remediation with source agencies?
- What would an intern own in the first month?
