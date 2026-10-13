# Databricks notebook source
# MAGIC %md
# MAGIC # 01_setup: platform and control framework
# MAGIC
# MAGIC Creates everything the pipeline needs before any data arrives: catalog, one schema per layer, the landing volume,
# MAGIC the `ops` control tables, and the seed config (sources + DQ rules).
# MAGIC
# MAGIC **Idempotent:** every statement is `IF NOT EXISTS` or `MERGE`, so running it 10 times gives the same result as running it once.
# MAGIC Re-running never resets watermarks or deletes run history.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catalog, schemas, volume

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS msf_demo COMMENT 'MSF social assistance lakehouse demo (synthetic data)';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS msf_demo.landing COMMENT 'Raw files as received (Volumes)';
# MAGIC CREATE SCHEMA IF NOT EXISTS msf_demo.bronze  COMMENT 'Raw tables, all STRING, plus ingest metadata. Pipeline access only (contains raw PII)';
# MAGIC CREATE SCHEMA IF NOT EXISTS msf_demo.silver  COMMENT 'Cleansed, deduplicated, DQ-checked, PII hashed/encrypted';
# MAGIC CREATE SCHEMA IF NOT EXISTS msf_demo.gold    COMMENT 'Dimensional model for analytics';
# MAGIC CREATE SCHEMA IF NOT EXISTS msf_demo.ops     COMMENT 'Control, audit, DQ and remediation tables';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS msf_demo.landing.raw_files COMMENT 'Landing zone: <source>/batch_date=YYYY-MM-DD/<file>';

# COMMAND ----------

LANDING = "/Volumes/msf_demo/landing/raw_files"
SOURCES = ["beneficiary", "assistance_txn", "scheme", "office", "planning_area"]

for s in SOURCES:
    dbutils.fs.mkdirs(f"{LANDING}/{s}")
dbutils.fs.mkdirs(f"{LANDING}/_checkpoints")

display(dbutils.fs.ls(LANDING))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Control tables (`ops`)
# MAGIC Design and column meanings: `docs/control_framework.md`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS msf_demo.ops.ingestion_control (
# MAGIC   source_name       STRING  NOT NULL,
# MAGIC   source_path       STRING  NOT NULL,
# MAGIC   file_format       STRING  NOT NULL,
# MAGIC   reader_options    MAP<STRING, STRING>,
# MAGIC   bronze_table      STRING  NOT NULL,
# MAGIC   silver_table      STRING  NOT NULL,
# MAGIC   business_key      STRING  NOT NULL,
# MAGIC   load_type         STRING  NOT NULL,
# MAGIC   watermark_value   TIMESTAMP,
# MAGIC   is_active         BOOLEAN NOT NULL,
# MAGIC   owner             STRING,
# MAGIC   updated_ts        TIMESTAMP
# MAGIC ) COMMENT 'One row per source. Drives the metadata-driven bronze/silver loops';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS msf_demo.ops.batch_run_log (
# MAGIC   run_id         STRING NOT NULL,
# MAGIC   job_run_id     STRING,
# MAGIC   batch_date     DATE,
# MAGIC   layer          STRING,
# MAGIC   table_name     STRING,
# MAGIC   status         STRING NOT NULL,
# MAGIC   start_ts       TIMESTAMP,
# MAGIC   end_ts         TIMESTAMP,
# MAGIC   rows_in        BIGINT,
# MAGIC   rows_out       BIGINT,
# MAGIC   rows_rejected  BIGINT,
# MAGIC   error_msg      STRING
# MAGIC ) COMMENT 'One row per table load. Lifecycle STARTED -> SUCCESS | FAILED';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS msf_demo.ops.file_manifest (
# MAGIC   file_path    STRING NOT NULL,
# MAGIC   source_name  STRING,
# MAGIC   batch_date   DATE,
# MAGIC   file_size    BIGINT,
# MAGIC   file_mod_ts  TIMESTAMP,
# MAGIC   row_count    BIGINT,
# MAGIC   run_id       STRING,
# MAGIC   ingest_ts    TIMESTAMP
# MAGIC ) COMMENT 'Every file ingested into bronze, and by which run';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS msf_demo.ops.dq_rule (
# MAGIC   rule_id      STRING  NOT NULL,
# MAGIC   target_table STRING  NOT NULL,
# MAGIC   column_name  STRING,
# MAGIC   rule_expr    STRING  NOT NULL,
# MAGIC   severity     STRING  NOT NULL,
# MAGIC   description  STRING,
# MAGIC   is_active    BOOLEAN NOT NULL
# MAGIC ) COMMENT 'DQ rules as data. rule_expr is a SQL boolean: TRUE = pass, NULL = fail';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS msf_demo.ops.dq_result (
# MAGIC   run_id        STRING,
# MAGIC   rule_id       STRING,
# MAGIC   target_table  STRING,
# MAGIC   severity      STRING,
# MAGIC   rows_checked  BIGINT,
# MAGIC   rows_failed   BIGINT,
# MAGIC   result_ts     TIMESTAMP
# MAGIC ) COMMENT 'Per rule, per run: how many rows were checked and how many failed';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS msf_demo.ops.remediation_log (
# MAGIC   remediation_id  STRING,
# MAGIC   scenario        STRING,
# MAGIC   target_table    STRING,
# MAGIC   action          STRING,
# MAGIC   rows_affected   BIGINT,
# MAGIC   reason          STRING,
# MAGIC   performed_by    STRING,
# MAGIC   run_id          STRING,
# MAGIC   performed_ts    TIMESTAMP
# MAGIC ) COMMENT 'Audit trail of every fix: replay, rollback, inferred member, backfill';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Seed config
# MAGIC Config lives here as code (and therefore in Git), then gets `MERGE`d into the tables.
# MAGIC - New source or rule: add a line and re-run.
# MAGIC - Removed rule: deactivated (`is_active = false`), never deleted, so past `dq_result` rows still make sense.
# MAGIC - Watermarks are **never** touched by setup.

# COMMAND ----------

sources = [
    # source_name,     file_format, reader_options,                     bronze_table,               silver_table,                business_key,     load_type
    ("beneficiary",    "csv",  {"header": "true"},                      "bronze.brz_beneficiary",    "silver.slv_beneficiary",    "beneficiary_id", "incremental"),
    ("assistance_txn", "json", {"multiLine": "false"},                  "bronze.brz_assistance_txn", "silver.slv_assistance_txn", "txn_id",         "incremental"),
    ("scheme",         "csv",  {"header": "true"},                      "bronze.brz_scheme",         "silver.slv_scheme",         "scheme_code",    "incremental"),
    ("office",         "json", {"multiLine": "true"},                   "bronze.brz_office",         "silver.slv_office",         "office_code",    "full"),
    ("planning_area",  "csv",  {"header": "true"},                      "bronze.brz_planning_area",  "silver.slv_location",       "planning_area",  "full"),
]

rows = [(name, f"{LANDING}/{name}/", fmt, opts, brz, slv, key, load, True, "data-eng")
        for name, fmt, opts, brz, slv, key, load in sources]

spark.createDataFrame(rows, """source_name string, source_path string, file_format string,
    reader_options map<string,string>, bronze_table string, silver_table string,
    business_key string, load_type string, is_active boolean, owner string""").createOrReplaceTempView("seed_sources")

spark.sql("""
MERGE INTO msf_demo.ops.ingestion_control t
USING seed_sources s ON t.source_name = s.source_name
WHEN MATCHED THEN UPDATE SET
  source_path = s.source_path, file_format = s.file_format, reader_options = s.reader_options,
  bronze_table = s.bronze_table, silver_table = s.silver_table, business_key = s.business_key,
  load_type = s.load_type, is_active = s.is_active, owner = s.owner, updated_ts = current_timestamp()
  -- watermark_value deliberately NOT updated
WHEN NOT MATCHED THEN INSERT
  (source_name, source_path, file_format, reader_options, bronze_table, silver_table,
   business_key, load_type, watermark_value, is_active, owner, updated_ts)
  VALUES (s.source_name, s.source_path, s.file_format, s.reader_options, s.bronze_table, s.silver_table,
   s.business_key, s.load_type, NULL, s.is_active, s.owner, current_timestamp())
""")

display(spark.table("msf_demo.ops.ingestion_control"))

# COMMAND ----------

B, T = "silver.slv_beneficiary", "silver.slv_assistance_txn"
rules = [
    # rule_id,  target, column,                     rule_expr (TRUE = pass),                                                   severity, description
    ("BEN_001", B, "nric",                     "nric IS NOT NULL",                                                        "REJECT", "NRIC is mandatory"),
    ("BEN_002", B, "nric",                     "nric RLIKE '^[STFGM][0-9]{7}[A-Z]$'",                                     "REJECT", "NRIC format"),
    ("BEN_003", B, "dob",                      "dob IS NOT NULL AND dob <= current_date()",                               "REJECT", "Valid date of birth"),
    ("BEN_004", B, "postal_code",              "postal_code RLIKE '^[0-9]{6}$'",                                          "WARN",   "6-digit postal code"),
    ("BEN_005", B, "household_size",           "household_size BETWEEN 1 AND 15",                                         "REJECT", "Plausible household size"),
    ("BEN_006", B, "monthly_household_income", "monthly_household_income >= 0",                                           "REJECT", "Income not negative"),
    ("TXN_001", T, "amount",                   "amount > 0",                                                              "REJECT", "Positive amount"),
    ("TXN_002", T, "disbursement_date",        "disbursement_date IS NOT NULL AND disbursement_date <= current_date()",   "REJECT", "Valid disbursement date"),
    ("TXN_003", T, "scheme_code",              "max_monthly_amount IS NOT NULL",                                          "REJECT", "Scheme exists (after left join to slv_scheme)"),
    ("TXN_004", T, "amount",                   "amount <= max_monthly_amount",                                            "WARN",   "Amount within scheme cap"),
    ("TXN_005", T, "status",                   "status IN ('DISBURSED','PENDING','REVERSED')",                            "REJECT", "Known status"),
]

spark.createDataFrame([r + (True,) for r in rules], """rule_id string, target_table string, column_name string,
    rule_expr string, severity string, description string, is_active boolean""").createOrReplaceTempView("seed_rules")

spark.sql("""
MERGE INTO msf_demo.ops.dq_rule t
USING seed_rules s ON t.rule_id = s.rule_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN UPDATE SET is_active = false
""")

display(spark.table("msf_demo.ops.dq_rule").orderBy("rule_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Checks

# COMMAND ----------

# Secrets created in the smoke test must exist (values are never printed)
keys = {s.key for s in dbutils.secrets.list("msf_demo")}
assert {"aes_key", "hash_salt"} <= keys, f"Missing secrets, found: {keys}"

ops_tables = {r.tableName for r in spark.sql("SHOW TABLES IN msf_demo.ops").collect()}
expected = {"ingestion_control", "batch_run_log", "file_manifest", "dq_rule", "dq_result", "remediation_log"}
assert expected <= ops_tables, f"Missing: {expected - ops_tables}"

print("Setup OK:", sorted(ops_tables))
print("Sources:", spark.table("msf_demo.ops.ingestion_control").count(),
      "| Active rules:", spark.table("msf_demo.ops.dq_rule").where("is_active").count())
