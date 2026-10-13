# Databricks notebook source
# MAGIC %md
# MAGIC # 01_setup
# MAGIC
# MAGIC This notebook builds the empty platform: one schema per medallion layer, a landing volume for raw files,
# MAGIC and four small control tables that record what the pipeline did.
# MAGIC
# MAGIC It is **safe to re-run**. Every statement either checks if the object already exists or merges,
# MAGIC so running it twice gives the same result as running it once.
# MAGIC
# MAGIC It can also **reset** the pipeline: wipe everything built from the raw files, keep the raw files,
# MAGIC and rebuild from scratch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Settings
# MAGIC Everything is named from one catalog variable. If catalog creation is blocked, change it to `workspace`.

# COMMAND ----------

CATALOG = "msf_demo"
SECRET_SCOPE = "msf_demo"                                # created in the smoke test: aes_key, hash_salt

LANDING = f"/Volumes/{CATALOG}/landing/raw_files"        # raw files, exactly as received
CHECKPOINTS = f"/Volumes/{CATALOG}/ops/checkpoints"      # Auto Loader's memory of which files it has loaded

# One landing folder per source. Four synthetic (generate_data.py) + one real (data.gov.sg).
SOURCES = ["clients", "referrals", "programmes", "corrections", "population"]

# A dropdown instead of a text box, so the default is always the safe choice.
dbutils.widgets.dropdown("reset", "no", ["no", "yes"], "Reset pipeline first?")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional reset
# MAGIC Set the dropdown at the top to **yes** to drop bronze, silver, gold and ops.
# MAGIC
# MAGIC The checkpoints live in a volume inside `ops`, so they go too. That means the next bronze run re-reads every file.
# MAGIC
# MAGIC **Landing is never dropped.** The raw files are the source of truth, and every table can be rebuilt from them.

# COMMAND ----------

if dbutils.widgets.get("reset") == "yes":
    for schema in ["bronze", "silver", "gold", "ops"]:
        # CASCADE drops everything inside: tables, views, functions, volumes.
        spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema} CASCADE")
        print(f"dropped {CATALOG}.{schema}")
    print("Reset done. Set the dropdown back to 'no' so the next run doesn't wipe again.")
else:
    print("No reset.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog, schemas, volumes
# MAGIC One schema per layer, so you can see where any table belongs from its name alone,
# MAGIC and permissions can be granted per layer (e.g. analysts get gold only).

# COMMAND ----------

existing = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
if CATALOG not in existing:
    try:
        spark.sql(f"CREATE CATALOG {CATALOG}")
    except Exception as e:
        raise RuntimeError(f"Cannot create catalog '{CATALOG}'. Set CATALOG = 'workspace' in Settings and re-run.") from e

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.landing COMMENT 'Raw files as received'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze  COMMENT 'Raw rows as tables, all STRING. Contains raw PII, pipeline only'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver  COMMENT 'Cleaned, checked, deduplicated, PII protected'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold    COMMENT 'Star schema for reporting'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ops     COMMENT 'Run log, DQ rules and results, quarantine'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.raw_files")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.ops.checkpoints")

for s in SOURCES:
    dbutils.fs.mkdirs(f"{LANDING}/{s}")

display(dbutils.fs.ls(LANDING))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control tables
# MAGIC Four tables, each answering one question:
# MAGIC
# MAGIC | Table | Question it answers |
# MAGIC |---|---|
# MAGIC | `batch_run_log` | What ran, did it succeed, and how many rows went in and out? |
# MAGIC | `dq_rule` | Which quality checks apply? (rules are data, not code) |
# MAGIC | `dq_result` | How many rows failed each rule on each run? |
# MAGIC | `quarantine` | Which rows were rejected, why, and have they been fixed? |

# COMMAND ----------

# One row per step per run. A row stuck in STARTED means the step crashed mid-way.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.ops.batch_run_log (
  run_id         STRING    COMMENT 'One id per pipeline run, shared by all its steps',
  batch_label    STRING    COMMENT 'Which batch was processed, e.g. batch1',
  step           STRING    COMMENT 'bronze | silver | gold | remediation',
  table_name     STRING,
  status         STRING    COMMENT 'STARTED | SUCCESS | FAILED',
  started_at     TIMESTAMP,
  ended_at       TIMESTAMP,
  rows_in        BIGINT,
  rows_out       BIGINT,
  rows_rejected  BIGINT,
  note           STRING    COMMENT 'Free text, e.g. what a remediation run fixed',
  error_msg      STRING
)""")

# Each rule is a SQL condition that is TRUE for a good row.
# A NULL result counts as a failure, so a missing value can never slip through.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.ops.dq_rule (
  rule_id      STRING,
  table_name   STRING,
  column_name  STRING,
  rule_expr    STRING   COMMENT 'SQL condition, TRUE = pass',
  severity     STRING   COMMENT 'REJECT = quarantine the row | WARN = keep the row, count the failure',
  description  STRING,
  is_active    BOOLEAN
)""")

# Totals per rule per run. This is what a DQ dashboard reads.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.ops.dq_result (
  run_id        STRING,
  rule_id       STRING,
  table_name    STRING,
  rows_checked  BIGINT,
  rows_failed   BIGINT,
  checked_at    TIMESTAMP
)""")

# One table for all sources. The rejected row is stored whole as JSON, so the
# table works for clients and referrals alike. Replay turns it back into columns with from_json.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.ops.quarantine (
  run_id           STRING         COMMENT 'Run that rejected the row',
  table_name       STRING,
  business_key     STRING         COMMENT 'client_id or referral_id. Empty means it cannot be fixed by key',
  record_json      STRING         COMMENT 'The full rejected row',
  failed_rules     ARRAY<STRING>,
  status           STRING         COMMENT 'OPEN | RESOLVED',
  quarantined_at   TIMESTAMP,
  resolved_at      TIMESTAMP,
  resolved_run_id  STRING         COMMENT 'Remediation run that fixed it'
)""")

print("Control tables ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality rules
# MAGIC The rules are listed here, so they are versioned in Git, and then merged into `ops.dq_rule`.
# MAGIC Adding a check means adding one line. The silver code doesn't change.
# MAGIC
# MAGIC Rules run **after** silver has cleaned the data (trimmed text, fixed letter case, converted dates).
# MAGIC So a wrongly formatted date shows up here as NULL, and a messy status that cleaning fixed passes.
# MAGIC `client_exists` and `programme_exists` are true/false columns that silver adds by looking each id up.

# COMMAND ----------

C, R = "silver.slv_clients", "silver.slv_referrals"
rules = [
    # rule_id    table  column            rule_expr (TRUE = good row)                                         severity  description
    ("CLI_001", C, "client_id",      "client_id RLIKE '^C[0-9]{4}$'",                                         "REJECT", "client_id present and well formed"),
    ("CLI_002", C, "postal_code",    "postal_code RLIKE '^[0-9]{6}$'",                                        "REJECT", "Postal code is 6 digits"),
    ("CLI_003", C, "household_type", "household_type IN ('Single-parent','Two-parent','Elderly-living-alone',"
                                     "'Multigenerational','Couple-no-children')",                             "WARN",   "Known household type after cleaning"),
    ("REF_001", R, "referral_id",    "referral_id RLIKE '^R[0-9]{5}$'",                                       "REJECT", "referral_id present and well formed"),
    ("REF_002", R, "referral_date",  "referral_date IS NOT NULL AND referral_date <= current_date()",         "REJECT", "Date is yyyy-MM-dd and not in the future"),
    ("REF_003", R, "client_id",      "client_exists",                                                         "REJECT", "Referred client exists as a valid client in silver"),
    ("REF_004", R, "programme_id",   "programme_exists",                                                      "REJECT", "Programme exists"),
    ("REF_005", R, "status",         "status IN ('Submitted','Approved','Rejected','Closed')",                 "REJECT", "Known status after cleaning"),
]

spark.createDataFrame(
    [r + (True,) for r in rules],
    "rule_id string, table_name string, column_name string, rule_expr string, severity string, description string, is_active boolean",
).createOrReplaceTempView("new_rules")

# MERGE instead of INSERT, so re-running setup updates rules rather than duplicating them.
spark.sql(f"""
MERGE INTO {CATALOG}.ops.dq_rule t
USING new_rules s ON t.rule_id = s.rule_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

display(spark.table(f"{CATALOG}.ops.dq_rule").orderBy("rule_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks

# COMMAND ----------

# Secrets hold the encryption key and hashing salt. If they're missing, warn rather than fail:
# setup doesn't need them, and silver can fall back to a demo-only key.
try:
    keys = {s.key for s in dbutils.secrets.list(SECRET_SCOPE)}
    missing = {"aes_key", "hash_salt"} - keys
    print("Secrets OK." if not missing else f"WARNING: secret scope '{SECRET_SCOPE}' is missing {missing}.")
except Exception:
    print(f"WARNING: secret scope '{SECRET_SCOPE}' not found. Silver will use a demo-only key.")

# isTemporary filter: SHOW TABLES also lists session temp views such as new_rules.
tables = sorted(r.tableName for r in spark.sql(f"SHOW TABLES IN {CATALOG}.ops").collect() if not r.isTemporary)
assert tables == ["batch_run_log", "dq_result", "dq_rule", "quarantine"], f"Unexpected ops tables: {tables}"

print("Setup OK.")
print("ops tables:", tables)
print("active rules:", spark.table(f"{CATALOG}.ops.dq_rule").where("is_active").count())
print("landing folders:", sorted(f.name.rstrip("/") for f in dbutils.fs.ls(LANDING)))
