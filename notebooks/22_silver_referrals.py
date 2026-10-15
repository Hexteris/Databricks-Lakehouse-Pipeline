# Databricks notebook source
# MAGIC %md
# MAGIC # 22_silver_referrals
# MAGIC
# MAGIC Same pattern as clients: **clean, deduplicate, check, quarantine, merge**. One row per referral.
# MAGIC
# MAGIC What's new here is checking against **other tables**. A referral is only valid if its client and its programme
# MAGIC exist in silver. So this notebook must run **after** `21_silver_clients` and `20_silver_reference`.
# MAGIC
# MAGIC Because "client exists" means "exists as a **valid** silver client", a referral for a quarantined client is
# MAGIC quarantined too. When the client is fixed, the referral can be released.
# MAGIC
# MAGIC After remediation, any quarantined referral that now loads into silver is marked RESOLVED.
# MAGIC
# MAGIC Referrals hold no direct PII (only ids), so there's no hashing or encryption in this step.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

SOURCE = f"{CATALOG}.bronze.brz_referrals"
TARGET = f"{CATALOG}.silver.slv_referrals"
RULES_FOR = "silver.slv_referrals"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Clean
# MAGIC `" approved "` becomes `Approved` and passes. `15/03/2026` isn't in the expected yyyy-MM-dd format,
# MAGIC so it becomes NULL here and the date rule catches it. We don't guess whether it's day/month or month/day.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW referrals_clean AS
SELECT
    nullif(trim(referral_id), '')                                     AS referral_id,
    nullif(trim(client_id), '')                                       AS client_id,
    nullif(trim(programme_id), '')                                    AS programme_id,
    CAST(try_to_timestamp(trim(referral_date), 'yyyy-MM-dd') AS DATE) AS referral_date,
    initcap(trim(status))                                             AS status,
    to_json(struct(*))                                                AS record_json,
    _source_file,
    _ingest_ts
FROM {SOURCE}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Deduplicate
# MAGIC Keep the newest copy of each referral. This removes a referral sent twice in the same file,
# MAGIC and keeps only the latest copy when an old referral is sent again in a later batch.
# MAGIC Rows with no `referral_id` are all kept, so each one is quarantined rather than silently lost.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW referrals_latest AS
SELECT * EXCEPT (rn)
FROM (
    SELECT *, row_number() OVER (PARTITION BY referral_id ORDER BY _ingest_ts DESC) AS rn
    FROM referrals_clean
)
WHERE rn = 1 OR referral_id IS NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Look up the client and programme, then check every row
# MAGIC The two LEFT JOINs add true/false columns, `client_exists` and `programme_exists`.
# MAGIC Rules REF_003 and REF_004 in `ops.dq_rule` simply test those columns.
# MAGIC Each id is unique in its silver table, so the joins never multiply rows.
# MAGIC
# MAGIC The rule-building code is the same as in clients. It's repeated on purpose, so each notebook reads top to bottom on its own.
# MAGIC In production I'd move it into a shared module.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW referrals_enriched AS
SELECT r.*,
       c.client_id    IS NOT NULL AS client_exists,
       p.programme_id IS NOT NULL AS programme_exists
FROM referrals_latest r
LEFT JOIN {CATALOG}.silver.slv_clients    c ON r.client_id    = c.client_id
LEFT JOIN {CATALOG}.silver.slv_programmes p ON r.programme_id = p.programme_id
""")

rules = (spark.table(f"{CATALOG}.ops.dq_rule")
         .where(f"table_name = '{RULES_FOR}' AND is_active")
         .orderBy("rule_id").collect())

def failures(severity):
    checks = [f"CASE WHEN coalesce({r.rule_expr}, false) THEN NULL ELSE '{r.rule_id}' END"
              for r in rules if r.severity == severity]
    if not checks:
        return "CAST(array() AS ARRAY<STRING>)"
    return f"array_compact(array({', '.join(checks)}))"

for r in rules:
    print(f"{r.rule_id} [{r.severity}] {r.rule_expr}")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW referrals_checked AS
SELECT *,
       {failures("REJECT")} AS reject_fails,
       {failures("WARN")}   AS warn_fails
FROM referrals_enriched
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 + 5. Record results, quarantine, merge
# MAGIC `row_hash` works as in clients: a referral re-sent unchanged doesn't update anything.
# MAGIC If its status changes later (e.g. Submitted to Approved), the hash changes and the row is updated.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    referral_id         STRING,
    client_id           STRING,
    programme_id        STRING,
    referral_date       DATE,
    status              STRING,
    row_hash            STRING    COMMENT 'Fingerprint of the values, used to detect real changes',
    _source_file        STRING,
    _ingest_ts          TIMESTAMP,
    _silver_run_id      STRING,
    _silver_updated_at  TIMESTAMP
)""")

# COMMAND ----------

log_start("silver", TARGET)
try:
    rows_in  = spark.table(SOURCE).count()
    latest   = spark.table("referrals_latest").count()
    rejected = spark.sql("SELECT count(*) FROM referrals_checked WHERE size(reject_fails) > 0").first()[0]

    # 4a. One row per rule: checked and failed
    spark.sql(f"INSERT INTO {CATALOG}.ops.dq_result " + " UNION ALL ".join(
        f"""SELECT '{RUN_ID}', '{r.rule_id}', '{RULES_FOR}', count(*),
                   count_if(array_contains(concat(reject_fails, warn_fails), '{r.rule_id}')), current_timestamp()
            FROM referrals_checked""" for r in rules))

    # 4b. Rejected rows to quarantine, unless the same original row is already there
    q = spark.sql(f"""
        MERGE INTO {CATALOG}.ops.quarantine t
        USING (
            SELECT '{RUN_ID}' AS run_id, '{RULES_FOR}' AS table_name, coalesce(referral_id, '') AS business_key,
                   record_json, reject_fails AS failed_rules
            FROM referrals_checked WHERE size(reject_fails) > 0
        ) s
        ON t.table_name = s.table_name AND t.record_json = s.record_json
        WHEN NOT MATCHED THEN INSERT (run_id, table_name, business_key, record_json, failed_rules, status, quarantined_at)
        VALUES (s.run_id, s.table_name, s.business_key, s.record_json, s.failed_rules, 'OPEN', current_timestamp())
    """).first()

    # 5. Merge the good rows into silver
    m = spark.sql(f"""
        MERGE INTO {TARGET} t
        USING (
            SELECT
                referral_id, client_id, programme_id, referral_date, status,
                sha2(concat_ws('|', client_id, programme_id, CAST(referral_date AS STRING), status), 256) AS row_hash,
                _source_file,
                _ingest_ts,
                '{RUN_ID}'          AS _silver_run_id,
                current_timestamp() AS _silver_updated_at
            FROM referrals_checked
            WHERE size(reject_fails) = 0
        ) s
        ON t.referral_id = s.referral_id
        WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).first()

    # 6. Release from quarantine: a quarantined key that this run (or a later one) loaded into silver is fixed.
    #    "Loaded after it was quarantined" matters: if a bad NEW version of an existing record is rejected,
    #    the old silver row must not count as a fix.
    #    This also releases rows that never had a correction, e.g. referrals that were only blocked by their client.
    rel = spark.sql(f"""
        MERGE INTO {CATALOG}.ops.quarantine t
        USING (SELECT referral_id AS business_key, _silver_updated_at FROM {TARGET}) s
        ON  t.table_name = '{RULES_FOR}' AND t.business_key = s.business_key
        AND t.status IN ('OPEN', 'REPLAYED') AND s._silver_updated_at > t.quarantined_at
        WHEN MATCHED THEN UPDATE SET
            status = 'RESOLVED', resolved_at = current_timestamp(),
            resolved_run_id = coalesce(t.resolved_run_id, '{RUN_ID}')
    """).first()

    valid = latest - rejected
    note = (f"latest={latest} valid={valid} inserted={m.num_inserted_rows} updated={m.num_updated_rows} "
            f"unchanged={valid - m.num_inserted_rows - m.num_updated_rows} new_quarantine={q.num_inserted_rows} "
            f"released={rel.num_updated_rows}")
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
# MAGIC -- Batch 1: REF_001 = 1, REF_002 = 3, REF_003 = 4, REF_004 = 0, REF_005 = 0
# MAGIC SELECT run_id, rule_id, rows_checked, rows_failed, checked_at
# MAGIC FROM msf_demo.ops.dq_result
# MAGIC WHERE table_name = 'silver.slv_referrals'
# MAGIC ORDER BY checked_at DESC, rule_id
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Why each referral is in quarantine. For REF_003, show what happened to its client.
# MAGIC -- C0010 never appears as a client id: its row arrived with an empty id, so it's in quarantine under ''.
# MAGIC SELECT q.business_key                                AS referral_id,
# MAGIC        q.failed_rules,
# MAGIC        get_json_object(q.record_json, '$.client_id')     AS client_id,
# MAGIC        get_json_object(q.record_json, '$.referral_date') AS original_date,
# MAGIC        cq.failed_rules                               AS client_failed_rules
# MAGIC FROM msf_demo.ops.quarantine q
# MAGIC LEFT JOIN msf_demo.ops.quarantine cq
# MAGIC   ON cq.table_name = 'silver.slv_clients'
# MAGIC  AND cq.business_key = get_json_object(q.record_json, '$.client_id')
# MAGIC WHERE q.table_name = 'silver.slv_referrals'
# MAGIC ORDER BY q.business_key

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reconciliation: every distinct bronze referral is either in silver or still in quarantine.
# MAGIC -- Rows with no id are counted one by one, because each one is quarantined separately.
# MAGIC -- RESOLVED rows are excluded: they are now in silver.
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM msf_demo.silver.slv_referrals)                                       AS in_silver,
# MAGIC   (SELECT count(*) FROM msf_demo.ops.quarantine
# MAGIC     WHERE table_name = 'silver.slv_referrals' AND status <> 'RESOLVED')                      AS in_quarantine,
# MAGIC   (SELECT count(DISTINCT nullif(trim(referral_id), '')) + count_if(nullif(trim(referral_id), '') IS NULL)
# MAGIC      FROM msf_demo.bronze.brz_referrals)                                                     AS distinct_in_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Messy statuses after cleaning. Expect only the 4 known values.
# MAGIC SELECT status, count(*) AS referrals FROM msf_demo.silver.slv_referrals GROUP BY status ORDER BY status

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name, status, rows_in, rows_out, rows_rejected, note, error_msg
    FROM {CATALOG}.ops.batch_run_log WHERE run_id = '{RUN_ID}'"""))
