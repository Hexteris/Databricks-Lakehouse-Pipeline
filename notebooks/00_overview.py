# Databricks notebook source
# MAGIC %md
# MAGIC # MSF Lakehouse Demo: social service referrals
# MAGIC
# MAGIC **The question:** how do you ingest data about vulnerable clients so that it can be trusted, personal data is protected,
# MAGIC history stays correct, and mistakes can be fixed?
# MAGIC
# MAGIC **The demo:** one re-runnable pipeline on Databricks, from raw files to a star schema, run across three batches that
# MAGIC contain changes, bad rows, and a corrections file.
# MAGIC
# MAGIC *Client and referral data is synthetic. Programme and agency names are illustrative and do not represent real MSF schemes or data.
# MAGIC The only real dataset is Census of Population 2020 residents by planning area (data.gov.sg).*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture
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
# MAGIC  ops      batch_run_log · dq_rule · dq_result · quarantine
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where each concept lives
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lake, warehouse, lakehouse, mesh
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## How data mesh could apply at MSF
# MAGIC
# MAGIC | Mesh principle | What it would look like |
# MAGIC |---|---|
# MAGIC | **Domain ownership** | each service area (e.g. social support, family services, community care) owns its data, the people closest to it |
# MAGIC | **Data as a product** | each domain publishes a documented, quality-checked gold table, e.g. a referrals product with an owner and a DQ score |
# MAGIC | **Self-serve platform** | a central team provides the template this demo shows: landing, bronze/silver/gold, run log, DQ rules, quarantine |
# MAGIC | **Federated governance** | common rules set centrally (PII classes, masks, retention), applied in every domain through Unity Catalog. E.g. a catalog per domain, shared groups like `pii_readers` |
# MAGIC
# MAGIC **When mesh is a bad idea:** few data teams, low data maturity, or mostly cross-domain questions. Then a central platform
# MAGIC is simpler, and you can still borrow the product thinking. For sensitive citizen data, governance must stay strong and
# MAGIC consistent whichever model is chosen.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What I'd change for production
# MAGIC - **Deployment:** Databricks Asset Bundles with CI/CD across dev, test and prod. Tests on the transformation SQL.
# MAGIC - **Scale:** stream bronze → silver instead of re-reading bronze; incremental MERGE into the fact; liquid clustering on large tables.
# MAGIC - **Access:** groups and service principals only; bronze readable by the pipeline only; row filters per office.
# MAGIC - **Keys:** secret scope backed by a cloud key vault, with a rotation process.
# MAGIC - **Operations:** alerts on FAILED runs and on DQ failure-rate spikes; a dashboard on `dq_result` and `quarantine`.
# MAGIC - **Declarative option:** Lakeflow Declarative Pipelines with expectations could replace the hand-written DQ step.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current state of the pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze'  AS layer, 'brz_clients'       AS tbl, count(*) AS rows FROM msf_demo.bronze.brz_clients
# MAGIC UNION ALL SELECT 'bronze', 'brz_referrals',     count(*) FROM msf_demo.bronze.brz_referrals
# MAGIC UNION ALL SELECT 'silver', 'slv_clients',       count(*) FROM msf_demo.silver.slv_clients
# MAGIC UNION ALL SELECT 'silver', 'slv_referrals',     count(*) FROM msf_demo.silver.slv_referrals
# MAGIC UNION ALL SELECT 'gold',   'dim_client',        count(*) FROM msf_demo.gold.dim_client
# MAGIC UNION ALL SELECT 'gold',   'fact_referral',     count(*) FROM msf_demo.gold.fact_referral
# MAGIC UNION ALL SELECT 'ops',    concat('quarantine ', status), count(*) FROM msf_demo.ops.quarantine GROUP BY status
