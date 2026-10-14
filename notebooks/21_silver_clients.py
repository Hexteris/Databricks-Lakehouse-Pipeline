# Databricks notebook source
# MAGIC %md
# MAGIC # 21_silver_clients
# MAGIC
# MAGIC Turns raw client rows into a clean, checked, privacy-protected table: **one row per client, current state**.
# MAGIC
# MAGIC | Step | What happens |
# MAGIC |---|---|
# MAGIC | 1. Clean | trim spaces, fix letter case, digits-only phone, text to date |
# MAGIC | 2. Deduplicate | keep the newest version of each client |
# MAGIC | 3. Check | run every rule in `ops.dq_rule` against every row |
# MAGIC | 4. Record | rule totals into `ops.dq_result`, rejected rows into `ops.quarantine` |
# MAGIC | 5. Protect + merge | hash NRIC, encrypt phone and email, then MERGE the good rows into `silver.slv_clients` |
# MAGIC
# MAGIC Each run re-reads all of bronze. That's cheap at this size, and every step is written so a re-run changes nothing.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

SOURCE = f"{CATALOG}.bronze.brz_clients"
TARGET = f"{CATALOG}.silver.slv_clients"
RULES_FOR = "silver.slv_clients"           # table_name used in ops.dq_rule

# Keys come from the secret scope, so they never appear in code or Git.
# secret() is a SQL function: the value is fetched at query time and redacted in any output.
try:
    have = {s.key for s in dbutils.secrets.list(SECRET_SCOPE)}
except Exception:
    have = set()

if {"aes_key", "hash_salt"} <= have:
    AES_KEY = f"secret('{SECRET_SCOPE}', 'aes_key')"
    SALT    = f"secret('{SECRET_SCOPE}', 'hash_salt')"
    print("Using keys from secret scope.")
else:
    AES_KEY = "'demo-only-key-16'"          # 16 characters = AES-128. DEMO ONLY.
    SALT    = "'demo-only-salt'"
    print("WARNING: secret scope missing, using a demo-only key. Never do this in production.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Clean
# MAGIC Fix what can be fixed safely, so the rules only catch real problems.
# MAGIC `  TWO-PARENT ` becomes `Two-parent` here and passes. A 5-digit postal code can't be fixed by formatting, so the rules catch it.
# MAGIC
# MAGIC `record_json` keeps the **original** bronze row, untouched. If the row is quarantined, that's what gets stored for later repair.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW clients_clean AS
SELECT
    nullif(trim(client_id), '')                                  AS client_id,
    trim(full_name)                                              AS full_name,
    upper(trim(nric_fake))                                       AS nric,
    regexp_replace(phone, '[^0-9]', '')                          AS phone,
    lower(trim(email))                                           AS email,
    trim(planning_area)                                          AS planning_area,
    initcap(trim(household_type))                                AS household_type,
    trim(postal_code)                                            AS postal_code,
    -- try_ returns NULL for a bad date instead of failing the whole run
    CAST(try_to_timestamp(trim(updated_date), 'yyyy-MM-dd') AS DATE) AS updated_date,
    to_json(struct(*))                                           AS record_json,
    _source_file,
    _ingest_ts
FROM {SOURCE}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Deduplicate
# MAGIC Bronze holds every version a client was ever sent. Keep the newest one per `client_id`.
# MAGIC This matters because MERGE fails if two source rows have the same key.
# MAGIC
# MAGIC Rows with **no** `client_id` are all kept, not deduplicated. Otherwise they would collapse into one row
# MAGIC and the others would vanish without a trace. Keeping them means the rules can quarantine every one.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW clients_latest AS
SELECT * EXCEPT (rn)
FROM (
    SELECT *,
           row_number() OVER (PARTITION BY client_id
                              ORDER BY updated_date DESC NULLS LAST, _ingest_ts DESC) AS rn
    FROM clients_clean
)
WHERE rn = 1 OR client_id IS NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Check every row against the rules
# MAGIC The rules are read from `ops.dq_rule`, not written here. Each rule's SQL condition becomes one check:
# MAGIC if the condition is not TRUE (false **or** NULL), the rule id is added to the row's list of failures.
# MAGIC
# MAGIC - `reject_fails`: REJECT rules the row broke. Any at all, and the row goes to quarantine.
# MAGIC - `warn_fails`: WARN rules the row broke. The row is kept, and the failure is only counted.

# COMMAND ----------

rules = (spark.table(f"{CATALOG}.ops.dq_rule")
         .where(f"table_name = '{RULES_FOR}' AND is_active")
         .orderBy("rule_id").collect())

def failures(severity):
    checks = [f"CASE WHEN coalesce({r.rule_expr}, false) THEN NULL ELSE '{r.rule_id}' END"
              for r in rules if r.severity == severity]
    if not checks:
        return "CAST(array() AS ARRAY<STRING>)"
    # array_compact drops the NULLs, leaving only the ids of rules that failed
    return f"array_compact(array({', '.join(checks)}))"

for r in rules:
    print(f"{r.rule_id} [{r.severity}] {r.rule_expr}")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW clients_checked AS
SELECT *,
       {failures("REJECT")} AS reject_fails,
       {failures("WARN")}   AS warn_fails
FROM clients_latest
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 + 5. Record results, quarantine, protect and merge
# MAGIC
# MAGIC **Quarantine** uses MERGE on the original row, so a re-run doesn't add the same bad row twice.
# MAGIC
# MAGIC **PII protection** happens on the way into silver. The raw values never land in a silver table:
# MAGIC
# MAGIC | Column | Treatment | Why |
# MAGIC |---|---|---|
# MAGIC | NRIC | salted SHA-256 hash, plus a masked display form | One-way and identical every time, so it still works as a matching key |
# MAGIC | phone, email | AES encryption | Reversible with the key, for staff who need to contact the client |
# MAGIC
# MAGIC **`row_hash`** is a fingerprint of the client's values. The MERGE only updates a row when the fingerprint changes.
# MAGIC Encryption gives a different result every time, so without this check every re-run would rewrite every row.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    client_id           STRING,
    full_name           STRING,
    nric_hash           STRING   COMMENT 'sha2(salt + NRIC). Join key, not reversible',
    nric_masked         STRING   COMMENT 'Display form, e.g. FAKE-***4567',
    phone_enc           STRING   COMMENT 'AES encrypted, base64',
    email_enc           STRING   COMMENT 'AES encrypted, base64',
    planning_area       STRING,
    household_type      STRING,
    postal_code         STRING,
    updated_date        DATE,
    row_hash            STRING   COMMENT 'Fingerprint of the values, used to detect real changes',
    _source_file        STRING,
    _ingest_ts          TIMESTAMP,
    _silver_run_id      STRING   COMMENT 'Silver run that last inserted or changed this row',
    _silver_updated_at  TIMESTAMP
)""")

# COMMAND ----------

log_start("silver", TARGET)
try:
    rows_in  = spark.table(SOURCE).count()
    latest   = spark.table("clients_latest").count()
    rejected = spark.sql("SELECT count(*) FROM clients_checked WHERE size(reject_fails) > 0").first()[0]

    # 4a. One row per rule: how many rows were checked, how many failed
    spark.sql(f"INSERT INTO {CATALOG}.ops.dq_result " + " UNION ALL ".join(
        f"""SELECT '{RUN_ID}', '{r.rule_id}', '{RULES_FOR}', count(*),
                   count_if(array_contains(concat(reject_fails, warn_fails), '{r.rule_id}')), current_timestamp()
            FROM clients_checked""" for r in rules))

    # 4b. Rejected rows to quarantine, unless the same original row is already there
    q = spark.sql(f"""
        MERGE INTO {CATALOG}.ops.quarantine t
        USING (
            SELECT '{RUN_ID}' AS run_id, '{RULES_FOR}' AS table_name, coalesce(client_id, '') AS business_key,
                   record_json, reject_fails AS failed_rules
            FROM clients_checked WHERE size(reject_fails) > 0
        ) s
        ON t.table_name = s.table_name AND t.record_json = s.record_json
        WHEN NOT MATCHED THEN INSERT (run_id, table_name, business_key, record_json, failed_rules, status, quarantined_at)
        VALUES (s.run_id, s.table_name, s.business_key, s.record_json, s.failed_rules, 'OPEN', current_timestamp())
    """).first()

    # 5. Protect PII and merge the good rows (WARN failures included) into silver
    m = spark.sql(f"""
        MERGE INTO {TARGET} t
        USING (
            SELECT
                client_id,
                full_name,
                sha2(concat({SALT}, nric), 256)                     AS nric_hash,
                concat(left(nric, 5), '***', right(nric, 4))        AS nric_masked,
                base64(aes_encrypt(phone, {AES_KEY}))               AS phone_enc,
                base64(aes_encrypt(email, {AES_KEY}))               AS email_enc,
                planning_area,
                household_type,
                postal_code,
                updated_date,
                sha2(concat_ws('|', {SALT}, full_name, nric, phone, email,
                               planning_area, household_type, postal_code), 256) AS row_hash,
                _source_file,
                _ingest_ts,
                '{RUN_ID}'          AS _silver_run_id,
                current_timestamp() AS _silver_updated_at
            FROM clients_checked
            WHERE size(reject_fails) = 0
        ) s
        ON t.client_id = s.client_id
        WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).first()

    valid = latest - rejected
    note = (f"latest={latest} valid={valid} inserted={m.num_inserted_rows} updated={m.num_updated_rows} "
            f"unchanged={valid - m.num_inserted_rows - m.num_updated_rows} new_quarantine={q.num_inserted_rows}")
    log_end(TARGET, "SUCCESS", rows_in=rows_in, rows_out=valid, rows_rejected=rejected, note=note)
    print(f"{TARGET}: bronze={rows_in} | {note}")
except Exception as e:
    log_end(TARGET, "FAILED", error_msg=str(e)[:1000])
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rule results for this table, newest run first. Batch 1: CLI_001 = 1, CLI_002 = 2, CLI_003 = 0.
# MAGIC SELECT run_id, rule_id, rows_checked, rows_failed, checked_at
# MAGIC FROM msf_demo.ops.dq_result
# MAGIC WHERE table_name = 'silver.slv_clients'
# MAGIC ORDER BY checked_at DESC, rule_id
# MAGIC LIMIT 6

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What is in quarantine, and why. Batch 1: empty client_id, C0025, C0060.
# MAGIC SELECT business_key, failed_rules, status, quarantined_at,
# MAGIC        get_json_object(record_json, '$.postal_code') AS original_postal_code
# MAGIC FROM msf_demo.ops.quarantine
# MAGIC WHERE table_name = 'silver.slv_clients'
# MAGIC ORDER BY business_key

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PII as an analyst sees it in silver: no readable NRIC, phone or email.
# MAGIC SELECT client_id, full_name, nric_masked, left(nric_hash, 16) AS nric_hash_start,
# MAGIC        left(phone_enc, 20) AS phone_enc_start, planning_area, household_type
# MAGIC FROM msf_demo.silver.slv_clients
# MAGIC ORDER BY client_id
# MAGIC LIMIT 5

# COMMAND ----------

# Someone holding the key can still read the phone number back.
display(spark.sql(f"""
    SELECT client_id, CAST(aes_decrypt(unbase64(phone_enc), {AES_KEY}) AS STRING) AS phone_decrypted
    FROM {TARGET} ORDER BY client_id LIMIT 3"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Messy household types from bronze, after cleaning. Expect only the 5 known values in batch 1.
# MAGIC SELECT household_type, count(*) AS clients
# MAGIC FROM msf_demo.silver.slv_clients
# MAGIC GROUP BY household_type
# MAGIC ORDER BY household_type

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name, status, rows_in, rows_out, rows_rejected, note, error_msg
    FROM {CATALOG}.ops.batch_run_log WHERE run_id = '{RUN_ID}'"""))
