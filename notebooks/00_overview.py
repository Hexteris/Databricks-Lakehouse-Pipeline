# Databricks notebook source
# MAGIC %md
# MAGIC # MSF Lakehouse Demo: social service referrals
# MAGIC
# MAGIC **The question:** how do you ingest data about vulnerable clients so that it can be **trusted**, personal data is **protected**,
# MAGIC history stays **correct**, and mistakes can be **fixed**?
# MAGIC
# MAGIC **The demo:** one re-runnable pipeline on Databricks, from raw files to a star schema, run across three batches that arrive
# MAGIC over time and contain changes, bad rows and a corrections file.
# MAGIC
# MAGIC *Client and referral data is synthetic. Programme and agency names are illustrative and do not represent real MSF schemes or data.
# MAGIC The only real dataset is Census of Population 2020 residents by planning area (data.gov.sg).*
# MAGIC
# MAGIC **How to read this notebook:** sections 1–4 explain the moving parts, section 5 walks through every notebook in run order,
# MAGIC sections 6–7 follow the data through the three batches and two individual clients, and sections 8–11 cover the concepts and frameworks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Architecture
# MAGIC
# MAGIC ```
# MAGIC  landing volume       raw files exactly as received, one folder per source   (never changed: source of truth)
# MAGIC        │  Auto Loader, checkpoint per source → each file loaded exactly once
# MAGIC        ▼
# MAGIC  bronze   brz_*       all text + _source_file, _ingest_ts, _run_id           (append-only, raw PII, pipeline only)
# MAGIC        │  clean → dedup → rules from ops.dq_rule → quarantine → hash / encrypt PII → MERGE real changes only
# MAGIC        ▼
# MAGIC  silver   slv_*       one row per client / referral, current state
# MAGIC        │  SCD1 / SCD2 MERGE, point-in-time join
# MAGIC        ▼
# MAGIC  gold     dim_date · dim_programme · dim_location · dim_client (SCD2, column masks)
# MAGIC           fact_referral (star)   ·   obt_referral_wide (wide, no PII)
# MAGIC
# MAGIC  ops      batch_run_log · dq_rule · dq_result · quarantine          (what the pipeline did, and why)
# MAGIC        ▲
# MAGIC        └── 40_remediation reads corrections, replays fixed rows back into bronze
# MAGIC ```
# MAGIC
# MAGIC | Layer | Holds | Rule it follows | Who may read it (production) |
# MAGIC |---|---|---|---|
# MAGIC | Landing | files, unchanged | never edited; every table can be rebuilt from here | pipeline |
# MAGIC | Bronze | one table per source, every column as text | append-only; never cleaned | pipeline only (raw PII) |
# MAGIC | Silver | one row per business key, current state | cleaned, checked, PII hashed or encrypted | data engineers, analysts with need |
# MAGIC | Gold | star schema + wide table | shaped for questions, history kept, masks applied | analysts, BI, ML |
# MAGIC | Ops | control and audit tables | written by every step | data engineers, auditors |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The sources
# MAGIC
# MAGIC | Source | Columns | Business key | Origin | Arrives |
# MAGIC |---|---|---|---|---|
# MAGIC | programmes | programme_id, programme_name, agency_name | `programme_id` (P01–P12) | synthetic | batch 1 |
# MAGIC | clients | client_id, full_name, nric_fake, phone, email, planning_area, household_type, postal_code, updated_date | `client_id` (C0001–C0210) | synthetic | batches 1, 2 |
# MAGIC | referrals | referral_id, client_id, programme_id, referral_date, status | `referral_id` (R00001–R00700) | synthetic | batches 1, 2 |
# MAGIC | corrections | target_table, business_key, column_name, corrected_value | n/a | synthetic | batch 3 |
# MAGIC | population | planning area / subzone rows × ~150 age and floor-area columns | planning area | **real**, Census 2020 | batch 1 |
# MAGIC
# MAGIC **What a referral is:** one event, "client X was referred to programme Y on date Z, and its status is S".
# MAGIC Referrals become the **fact table**; clients, programmes, dates and planning areas become the **dimensions** that describe each referral.
# MAGIC
# MAGIC **Why synthetic:** the demo needs realistic personal data to protect, and planted defects so every rule has something to catch.
# MAGIC Real personal data must never be used. The generator (`src/generate_data.py`) is seeded, so every run produces identical files,
# MAGIC and it writes `PLANTED_DEFECTS.txt` with the expected result of every step. The data is internally consistent:
# MAGIC postal sectors match planning areas, programmes fit the household (no childcare for someone living alone), and no referral predates its client.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The control tables (`ops`)
# MAGIC
# MAGIC | Table | Answers | One row per | Key columns |
# MAGIC |---|---|---|---|
# MAGIC | `batch_run_log` | What ran, did it succeed, how many rows? | step × table × run | run_id, batch_label, step, table_name, status, rows_in / out / rejected, note, error_msg |
# MAGIC | `dq_rule` | Which checks apply? | rule | rule_id, table_name, rule_expr (SQL, TRUE = pass), severity (REJECT / WARN), is_active |
# MAGIC | `dq_result` | How many rows failed each rule? | rule × run | run_id, rule_id, rows_checked, rows_failed |
# MAGIC | `quarantine` | Which rows were rejected, why, and were they fixed? | rejected row | business_key, record_json (the original row), failed_rules, status, quarantined_at, resolved_at |
# MAGIC
# MAGIC **Quarantine lifecycle:** `OPEN` (rejected) → `REPLAYED` (a correction was sent back through bronze) → `RESOLVED` (the row actually made it into silver).
# MAGIC A row is never marked RESOLVED without proof that it loaded.
# MAGIC
# MAGIC **The 8 rules**
# MAGIC
# MAGIC | Rule | Table | Check | Severity |
# MAGIC |---|---|---|---|
# MAGIC | CLI_001 | clients | `client_id` is C + 4 digits | REJECT |
# MAGIC | CLI_002 | clients | postal code is 6 digits | REJECT |
# MAGIC | CLI_003 | clients | household type is one of 5 known values (after cleaning) | WARN |
# MAGIC | REF_001 | referrals | `referral_id` is R + 5 digits | REJECT |
# MAGIC | REF_002 | referrals | date is yyyy-MM-dd and not in the future | REJECT |
# MAGIC | REF_003 | referrals | client exists **as a valid silver client** | REJECT |
# MAGIC | REF_004 | referrals | programme exists | REJECT |
# MAGIC | REF_005 | referrals | status is one of 4 known values (after cleaning) | REJECT |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. What happens every time a notebook runs
# MAGIC
# MAGIC Every pipeline notebook starts with `%run ./00_common`, which:
# MAGIC 1. Defines the names (`CATALOG = msf_demo`, landing and checkpoint paths, secret scope).
# MAGIC 2. Reads the `batch_label` widget. A Job passes it as a parameter; by hand you type it.
# MAGIC 3. Creates a new **`RUN_ID`** (a UUID). Every row the run writes carries it, so one load can be traced or removed.
# MAGIC 4. Defines `log_start()` and `log_end()`.
# MAGIC
# MAGIC Then, for each table the notebook builds:
# MAGIC
# MAGIC ```
# MAGIC log_start  → batch_run_log row: STARTED
# MAGIC try:  do the work (load / clean / check / MERGE)
# MAGIC       log_end → SUCCESS, rows_in, rows_out, rows_rejected, note ("inserted=10 updated=35 ...")
# MAGIC except: log_end → FAILED, error_msg, then re-raise so the Job sees the failure and can retry
# MAGIC ```
# MAGIC A row stuck in STARTED means the process died mid-way: visible, not silent.
# MAGIC
# MAGIC **Idempotency, layer by layer:** bronze uses the Auto Loader checkpoint (each file once); silver MERGEs on the business key and only
# MAGIC writes when `row_hash` changes; quarantine MERGEs on the original row; gold MERGEs dimensions and rebuilds the fact from silver.
# MAGIC Result: re-running any notebook, or the whole Job, with no new files changes nothing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Notebook by notebook, in run order
# MAGIC
# MAGIC ### `01_setup` (once, not part of the Job)
# MAGIC - Creates catalog `msf_demo`, schemas `landing`, `bronze`, `silver`, `gold`, `ops`, the `landing.raw_files` volume (one folder per source)
# MAGIC   and the `ops.checkpoints` volume.
# MAGIC - Creates the 4 control tables and MERGEs the 8 rules into `dq_rule` (MERGE, so re-running updates instead of duplicating).
# MAGIC - **Reset** dropdown: drops bronze, silver, gold and ops with CASCADE. Checkpoints live in ops, so they go too. **Landing is never dropped.**
# MAGIC   Next run re-reads every raw file and rebuilds everything.
# MAGIC - Excluded from the Job because a scheduled pipeline must never be able to wipe itself.
# MAGIC
# MAGIC ### `10_bronze`
# MAGIC For each of the 5 sources, `load_to_bronze(source, table)`:
# MAGIC 1. Skips the source if its landing folder is empty (Auto Loader can't infer a schema from nothing; corrections only arrive in batch 3).
# MAGIC 2. Reads new files with Auto Loader: CSV, header, **every column as text** (`inferColumnTypes = false`), schema stored per source.
# MAGIC 3. Adds `_source_file` (from `_metadata.file_path`), `_ingest_ts`, `_run_id`.
# MAGIC 4. Writes with `trigger(availableNow=True)` (process everything new, then stop), a **checkpoint** per source (which files are done),
# MAGIC    and `mergeSchema` (new columns allowed, per table).
# MAGIC 5. Counts rows carrying this `RUN_ID` and logs them.
# MAGIC
# MAGIC Output: `brz_programmes`, `brz_clients`, `brz_referrals`, `brz_corrections`, `brz_population`.
# MAGIC
# MAGIC ### `40_remediation` (does nothing until corrections exist)
# MAGIC 1. Shows which corrections match an **OPEN** quarantine row (by table + business key).
# MAGIC 2. For each table: collects the corrections per key into a map (`{postal_code: 382038}`), rebuilds the quarantined row from
# MAGIC    `record_json` with `from_json` using the bronze schema, takes the corrected value where there is one and the original otherwise.
# MAGIC 3. Appends the rebuilt rows **to bronze**, with `_source_file = 'remediation: <corrections file>'` and this run's id and time.
# MAGIC 4. Marks those quarantine rows **REPLAYED**. A re-run finds nothing OPEN to fix, so it replays 0.
# MAGIC
# MAGIC ### `20_silver_reference`
# MAGIC - `slv_programmes`: trim, keep the newest copy of each programme with `row_number()`. Rebuilt each run (`CREATE OR REPLACE`).
# MAGIC - `slv_population`: keep only planning-area rows (label ends in `-Total` with any spacing: catches `Changi- Total`), strip the suffix,
# MAGIC   turn census `-` into 0, keep total residents, residents 65+ and % aged 65+. 388 rows in → **55** planning areas out.
# MAGIC - Checks: sum of areas vs the national `Total` row within rounding tolerance (difference 40), and every client planning area exists in the census.
# MAGIC
# MAGIC ### `21_silver_clients`
# MAGIC | Step | Code | Why |
# MAGIC |---|---|---|
# MAGIC | Keys | `secret('msf_demo','aes_key')`, `secret('msf_demo','hash_salt')` | never in code or Git; demo-only fallback with a warning |
# MAGIC | 1. Clean | trim, `initcap` household type, digits-only phone, lowercase email, `try_to_timestamp` | fix what formatting can fix, so rules only catch real problems |
# MAGIC | | `to_json(struct(*)) AS record_json` | keep the **original** bronze row for quarantine and repair |
# MAGIC | 2. Dedup | `row_number()` per client_id, newest `updated_date` then `_ingest_ts`; rows with no id all kept | MERGE fails on duplicate keys; keyless rows must not collapse into one |
# MAGIC | 3. Rules | read `ops.dq_rule`, build `CASE WHEN coalesce(rule,false) THEN NULL ELSE 'rule_id' END` per rule | rules are data; NULL = fail |
# MAGIC | 4a | insert one `dq_result` row per rule | DQ trend over time |
# MAGIC | 4b | MERGE rejected rows into `quarantine` on `record_json` | the same bad row is never quarantined twice |
# MAGIC | 5. Protect | `sha2(salt + NRIC)`, `FAKE-***1823`, `base64(aes_encrypt(phone))`, same for email | raw PII never reaches silver |
# MAGIC | 5. MERGE | `WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE` | encryption output changes every time, so compare a fingerprint of plain values |
# MAGIC | 6. Release | quarantined keys now in silver, loaded **after** they were quarantined → RESOLVED | proves the fix landed |
# MAGIC
# MAGIC ### `22_silver_referrals`
# MAGIC Same pattern, no PII step, plus two LEFT JOINs that add `client_exists` (against **silver** clients) and `programme_exists`.
# MAGIC Because a referral needs a *valid* client, referrals of quarantined clients are quarantined too (**cascade**), and released automatically
# MAGIC once the client is fixed. Reconciliation: silver + still-open quarantine = distinct bronze referrals.
# MAGIC
# MAGIC ### `30_gold_dimensions`
# MAGIC - `dim_date`: every day 2020–2030 via `explode(sequence(...))`, key `yyyyMMdd`.
# MAGIC - `dim_programme`, `dim_location` (census figures): **SCD1** MERGE, overwrite on change, `<=>` null-safe compare, identity surrogate keys.
# MAGIC - `dim_client`: **SCD2**.
# MAGIC   - `scd_hash` over planning_area, household_type, postal_code. Different → close the current version, insert a new one.
# MAGIC   - `row_hash` (from silver) over everything. Different with the same `scd_hash` → overwrite in place (name, NRIC, phone, email).
# MAGIC   - Validity `[effective_from, effective_to)`; first version from 1900-01-01, current to 9999-12-31, `is_current`.
# MAGIC   - **Staged union**: changed clients appear twice in the MERGE source, once with `merge_key = client_id` (closes the old row),
# MAGIC     once with `merge_key = NULL` (never matches, so it's inserted as the new version). One MERGE, one transaction.
# MAGIC   - A preview counts new clients, new versions and Type 1 updates before the MERGE, for the log.
# MAGIC   - Checks: exactly one current version per client; no gaps or overlaps.
# MAGIC
# MAGIC ### `31_gold_facts`
# MAGIC - `fact_referral` (grain: one referral): LEFT JOIN to `dim_date`, to `dim_client` **on the version valid at the referral date**,
# MAGIC   to `dim_programme`, and to `dim_location` using **that version's** planning area. Missing lookups become `-1` (checks expect 0).
# MAGIC   Rebuilt each run, so released referrals and new versions are always reflected.
# MAGIC - `obt_referral_wide`: the fact joined to every dimension and flattened, with **no personal data**.
# MAGIC - Comparison: referrals per 100,000 residents by planning area, once on the star (with a join) and once on the wide table (no join).
# MAGIC
# MAGIC ### `50_governance`
# MAGIC Three SQL mask functions (`mask_name` → `T***`, `mask_postal` → `64****`, `mask_hidden` → `[hidden]`) attached with
# MAGIC `ALTER TABLE ... ALTER COLUMN ... SET MASK` to `dim_client`. Real values for members of `pii_readers` (or you, with the demo switch on).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The three batches: what changed in each layer
# MAGIC
# MAGIC | | Batch 1: initial load | Batch 2: changes | Batch 3: corrections |
# MAGIC |---|---|---|---|
# MAGIC | **Files** | programmes, clients_batch1, referrals_batch1, census | clients_batch2, referrals_batch2 | corrections_batch3 |
# MAGIC | **Bronze rows (total)** | clients 200, referrals 400 | clients 247, referrals 704 | clients 249, referrals 710 (+2 and +6 replayed) |
# MAGIC | **Silver clients** | 197 (3 quarantined) | 207: +10 new, 35 updated, 162 unchanged | 209: +2 released |
# MAGIC | **Silver referrals** | 392 (8 quarantined) | 687: +295 new, 5 quarantined | 697: +10 released |
# MAGIC | **Quarantine** | 11 OPEN | 16 OPEN | **12 RESOLVED, 4 OPEN** |
# MAGIC | **dim_client** | 197 versions, all current | 237 (207 current): +10 new, +30 versions, 5 Type 1 | 239 (+2 new clients) |
# MAGIC | **fact_referral** | 392 | 687 | 697 |
# MAGIC
# MAGIC **What was planted in each batch, and how the pipeline handled it**
# MAGIC
# MAGIC | Batch | Planted | Handled by | Result |
# MAGIC |---|---|---|---|
# MAGIC | 1 | empty client_id | CLI_001 | quarantined, no key → needs source fix |
# MAGIC | 1 | C0025 postal `12345`, C0060 `ABC123` | CLI_002 | quarantined, corrected in batch 3 |
# MAGIC | 1 | `"  TWO-PARENT "`, `" approved "`, `REJECTED` | cleaning | fixed, not rejected |
# MAGIC | 1 | empty referral_id | REF_001 | quarantined, needs source fix |
# MAGIC | 1 | dates `15/03/2026`, `March 5, 2026`, `2027-03-15` | REF_002 | quarantined, corrected in batch 3 |
# MAGIC | 1 | referrals of C0025, C0060, C0010 | REF_003 (cascade) | quarantined; C0025/C0060 ones released in batch 3 |
# MAGIC | 2 | 30 moves (incl. C0130) | SCD2 | new versions |
# MAGIC | 2 | 5 phone-only changes | SCD1 | overwritten, no new version |
# MAGIC | 2 | C0001, C0002 re-sent unchanged | `row_hash` | nothing written |
# MAGIC | 2 | C0201 `Single parent` | CLI_003 (WARN) | kept, failure counted |
# MAGIC | 2 | 3 referrals re-sent, R00406 twice in one file | dedup + `row_hash` | nothing written / one kept |
# MAGIC | 2 | R00441, R00442 for clients C9001, C9002 | REF_003 | quarantined, corrected to C0050, C0051 |
# MAGIC | 2 | R00521 `31/08/2026` | REF_002 | quarantined, corrected |
# MAGIC | 3 | 8 corrections | remediation replay | 8 corrected + 4 released with their client |
# MAGIC
# MAGIC **DQ failures per run** (from `dq_result`): CLI_002 2 → 2 → **0** · REF_002 3 → 4 → **0** · REF_003 4 → 8 → **2**.
# MAGIC Each run checks all rows, so batch 2's REF_003 of 8 = batch 1's 4 + 2 orphans + 2 new cascades.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Follow two clients end to end
# MAGIC
# MAGIC ### C0130: moves house (SCD2 and the point-in-time join)
# MAGIC | When | Layer | What happens |
# MAGIC |---|---|---|
# MAGIC | Batch 1 | bronze | arrives: Toa Payoh, postal `310870`, NRIC `FAKE-4761823`, phone `00002124` |
# MAGIC | | silver | passes every rule; stored as `nric_hash`, `FAKE-***1823`, `phone_enc`, `email_enc` |
# MAGIC | | gold | `dim_client` version **127**: Toa Payoh, 1900-01-01 → 9999-12-31, current |
# MAGIC | | fact | R00051 (Feb), R00140 (Mar), R00297 (May) → version 127, location Toa Payoh |
# MAGIC | Batch 2 | bronze | arrives again: Jurong West, postal `640870`, `updated_date` 2026-07-01 |
# MAGIC | | silver | `row_hash` differs → row updated in place (silver holds current state only) |
# MAGIC | | gold | `scd_hash` differs → version 127 closed with `effective_to = 2026-07-01`; new version: Jurong West from 2026-07-01, current |
# MAGIC | | fact | R00051, R00140, R00297 still → 127 (Toa Payoh). R00411 (Aug), R00585 (Sep) → new version (Jurong West) |
# MAGIC | Always | governance | analysts see `T***`, `64****`, `[hidden]`; `pii_readers` see real values |
# MAGIC
# MAGIC Without SCD2, all five referrals would count for Jurong West, and Toa Payoh's numbers would be wrong.
# MAGIC
# MAGIC ### C0025: arrives broken, gets fixed (DQ, cascade, remediation)
# MAGIC | When | Layer | What happens | Quarantine |
# MAGIC |---|---|---|---|
# MAGIC | Batch 1 | bronze | arrives with postal `12345` | |
# MAGIC | | silver clients | CLI_002 fails → rejected; original row stored as JSON | C0025 OPEN |
# MAGIC | | silver referrals | R00087, R00273 → REF_003 fails, client isn't valid | R00087, R00273 OPEN |
# MAGIC | Batch 2 | silver referrals | R00647 for C0025 → same cascade | R00647 OPEN |
# MAGIC | Batch 3 | bronze | correction `clients, C0025, postal_code, 382038` loaded | |
# MAGIC | | remediation | original row rebuilt with `382038`, appended to bronze as `remediation: ...` | C0025 REPLAYED |
# MAGIC | | silver clients | newest C0025 passes → inserted → release step | C0025 **RESOLVED** |
# MAGIC | | silver referrals | R00087, R00273, R00647 now find a valid client → inserted → released, **no correction of their own** | **RESOLVED** |
# MAGIC | | gold | C0025 gets its first `dim_client` version; its 3 referrals enter the fact | |
# MAGIC | Always | bronze | both C0025 rows stay: the original and its fix, side by side for audit | |
# MAGIC
# MAGIC **The 4 that stay OPEN:** the client with an empty id (was C0010), the referral with an empty id, and R00335 and R00609 (C0010's referrals).
# MAGIC Corrections are matched by business key, so rows without one can't be targeted: they need a fix in the source system.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. The PII journey
# MAGIC
# MAGIC | Column | Bronze | Silver | Gold `dim_client` (analyst view) | `obt_referral_wide` |
# MAGIC |---|---|---|---|---|
# MAGIC | NRIC | `FAKE-4761823` | `nric_hash` (salted SHA-256) + `nric_masked` | hash `[hidden]`, `FAKE-***1823` | not present |
# MAGIC | Name | plain | plain | `T***` | not present |
# MAGIC | Phone, email | plain | AES-GCM, base64 | `[hidden]` | not present |
# MAGIC | Postal code | plain | plain | `64****` | not present |
# MAGIC | Planning area, household | plain | plain | plain | plain |
# MAGIC
# MAGIC | Technique | Reversible? | Same input → same output? | Used for |
# MAGIC |---|---|---|---|
# MAGIC | Hash (salted) | no | yes | NRIC as a join key |
# MAGIC | Encrypt (AES-GCM) | yes, with the key | no (random IV) | contact details staff may need back |
# MAGIC | Mask (Unity Catalog) | data unchanged | depends on who asks | what each user sees at query time |
# MAGIC | Leave out | n/a | n/a | the wide table: the safest PII is the PII you never copy |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Where each concept lives
# MAGIC
# MAGIC | Concept | Notebook | Show |
# MAGIC |---|---|---|
# MAGIC | Medallion architecture | `10` → `2x` → `3x` | one schema per layer in Catalog Explorer |
# MAGIC | Control tables | `01_setup`, `00_common` | `ops.batch_run_log`, `ops.dq_rule` (rules are data) |
# MAGIC | Idempotent ingestion | `10_bronze` | re-run loads 0 rows; `_source_file` per row |
# MAGIC | Data quality + quarantine | `21`, `22` | `ops.dq_result`, `ops.quarantine`, cascade from clients to referrals |
# MAGIC | Hashing / encryption | `21_silver_clients` | `nric_hash`, `phone_enc`; decrypt with the key from the secret scope |
# MAGIC | Masking | `50_governance` | same query, masked vs unmasked |
# MAGIC | SCD Type 1 / Type 2 | `30_gold_dimensions` | client **C0130**: two versions, Toa Payoh → Jurong West |
# MAGIC | Star vs wide | `31_gold_facts` | the same question both ways (referrals per 100k residents) |
# MAGIC | Remediation | `40_remediation` | corrections replayed through bronze: 12 resolved, 4 need a source fix |
# MAGIC | Orchestration | Job `msf_pipeline` | task graph, `batch_label` parameter, retry on bronze |
# MAGIC
# MAGIC **The Job:** `bronze` → (`reference` ∥ `remediation` → `clients`) → `referrals` → `dimensions` → `facts` → `governance`.
# MAGIC `batch_label` is a job parameter passed to every widget of the same name; bronze retries once; setup is excluded.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Lake, warehouse, lakehouse, mesh
# MAGIC
# MAGIC | | Data lake | Data warehouse | Lakehouse | Data mesh |
# MAGIC |---|---|---|---|---|
# MAGIC | What it is | cheap files in object storage | managed database for SQL analytics | warehouse features on lake files | an **organisational** model, not a technology |
# MAGIC | Data | any format, raw | structured, modelled | any format, tables in an open format (Delta) | owned by domains as data products |
# MAGIC | Strength | cheap, flexible, ML-friendly | reliable, fast SQL, governed | both, on one copy of the data | scales ownership across many teams |
# MAGIC | Weakness | no transactions, easily becomes a "swamp" | costly, closed formats, weak for ML / unstructured | needs discipline in modelling and governance | needs mature teams and a strong platform; heavy for a small organisation |
# MAGIC
# MAGIC **Why this demo is a lakehouse:**
# MAGIC - **Delta Lake** adds a transaction log to plain files: ACID writes, `MERGE`, schema enforcement, time travel and `RESTORE`.
# MAGIC - **Unity Catalog** adds one governance layer over tables, volumes and functions: grants, lineage, column masks.
# MAGIC - The same tables serve SQL reporting (star schema) and ML (wide table), with no copy into a separate warehouse.
# MAGIC
# MAGIC ### How data mesh could apply at MSF
# MAGIC | Mesh principle | What it would look like |
# MAGIC |---|---|
# MAGIC | **Domain ownership** | each service area (e.g. social support, family services, community care) owns its data, the people closest to it |
# MAGIC | **Data as a product** | each domain publishes a documented, quality-checked gold table, e.g. a referrals product with an owner and a DQ score |
# MAGIC | **Self-serve platform** | a central team provides the template this demo shows: landing, bronze/silver/gold, run log, DQ rules, quarantine |
# MAGIC | **Federated governance** | common rules set centrally (PII classes, masks, retention), applied in every domain through Unity Catalog, e.g. a catalog per domain, shared groups like `pii_readers` |
# MAGIC
# MAGIC **When mesh is a bad idea:** few data teams, low data maturity, or mostly cross-domain questions. Then a central platform
# MAGIC is simpler, and you can still borrow the product thinking. For sensitive citizen data, governance must stay strong and
# MAGIC consistent whichever model is chosen.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Lessons from real problems, and what I'd change for production
# MAGIC
# MAGIC | What happened | Lesson |
# MAGIC |---|---|
# MAGIC | An Excel-saved "CSV" turned into binary junk columns | plain UTF-8 only; Delta schema enforcement kept bronze clean |
# MAGIC | `Changi- Total` silently dropped one planning area | reconcile against the source's own totals |
# MAGIC | Totals still differed by 40 | census rounds to 10: reconcile within a tolerance |
# MAGIC | My synthetic postal codes didn't match planning areas; 119 referrals didn't fit the household | sanity-check test data; fixed and rebuilt from scratch with the reset |
# MAGIC | Batches mislabelled when run by hand | the Job passes `batch_label` as a parameter |
# MAGIC | A skipped `dim_client` step left 4 referrals with `-1` keys | `-1` keys make gaps visible; the Job caught it up |
# MAGIC
# MAGIC **For production:** Databricks Asset Bundles with CI/CD across dev, test and prod; file-arrival triggers; streaming bronze → silver and an
# MAGIC incremental fact at scale; groups and service principals only, bronze readable by the pipeline only, row filters per office; keys in a
# MAGIC cloud key vault with rotation; alerts on FAILED runs and DQ spikes; optionally Lakeflow Declarative Pipelines with expectations for the DQ step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Current state (cheap, read-only)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze'  AS layer, 'brz_clients'       AS tbl, count(*) AS rows FROM msf_demo.bronze.brz_clients
# MAGIC UNION ALL SELECT 'bronze', 'brz_referrals',     count(*) FROM msf_demo.bronze.brz_referrals
# MAGIC UNION ALL SELECT 'silver', 'slv_clients',       count(*) FROM msf_demo.silver.slv_clients
# MAGIC UNION ALL SELECT 'silver', 'slv_referrals',     count(*) FROM msf_demo.silver.slv_referrals
# MAGIC UNION ALL SELECT 'gold',   'dim_client',        count(*) FROM msf_demo.gold.dim_client
# MAGIC UNION ALL SELECT 'gold',   'fact_referral',     count(*) FROM msf_demo.gold.fact_referral
# MAGIC UNION ALL SELECT 'ops',    concat('quarantine ', status), count(*) FROM msf_demo.ops.quarantine GROUP BY status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The run history: one line per step and batch
# MAGIC SELECT batch_label, step, count(*) AS tables, sum(rows_out) AS rows_out, count_if(status <> 'SUCCESS') AS not_success
# MAGIC FROM msf_demo.ops.batch_run_log
# MAGIC GROUP BY batch_label, step
# MAGIC ORDER BY min(started_at)
