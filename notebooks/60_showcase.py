# Databricks notebook source
# MAGIC %md
# MAGIC # Showcase: two clients, end to end
# MAGIC
# MAGIC | Client | Story | Proves |
# MAGIC |---|---|---|
# MAGIC | **C0130** | moves from Toa Payoh to Jurong West on 2026-07-01 | history stays correct (SCD2, point-in-time join, masks) |
# MAGIC | **C0025** | arrives with a bad postal code, blocks its referrals, gets corrected | data can be trusted, and mistakes can be fixed (DQ, cascade, remediation) |
# MAGIC
# MAGIC Every cell is read-only, so it's safe to run live.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C0130 · 1. The client's versions today
# MAGIC Batch 2 said C0130 moved. Instead of overwriting, the pipeline closed the old version and added a new one.
# MAGIC Area, household and postal code keep history (Type 2). Name and phone are overwritten (Type 1).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT client_sk, client_id, planning_area, household_type, effective_from, effective_to, is_current
# MAGIC FROM msf_demo.gold.dim_client
# MAGIC WHERE client_id = 'C0130'
# MAGIC ORDER BY effective_from

# COMMAND ----------

# MAGIC %md
# MAGIC ## C0130 · 2. Every batch is a version in the Delta history
# MAGIC Each MERGE into `dim_client` is a numbered version with its row counts.
# MAGIC Batch 1: 197 inserted. Batch 2: 40 inserted, 35 updated. Batch 3: 2 inserted. Job re-runs: 0 and 0 (idempotent).

# COMMAND ----------

from pyspark.sql import functions as F

history = (spark.sql("DESCRIBE HISTORY msf_demo.gold.dim_client")
           .where("operation = 'MERGE'")
           .select("version", "timestamp",
                   F.col("operationMetrics")["numTargetRowsInserted"].alias("rows_inserted"),
                   F.col("operationMetrics")["numTargetRowsUpdated"].alias("rows_updated"))
           .orderBy("version"))
display(history)

# The first MERGE into dim_client was batch 1
BATCH1_VERSION = history.first()["version"]
print("Batch 1 version:", BATCH1_VERSION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C0130 · 3. Time travel: the table as it was after batch 1
# MAGIC One version, Toa Payoh, current. Time travel is for operations (debugging, rollback). SCD2 is the permanent business history.

# COMMAND ----------

display(spark.sql(f"""
    SELECT client_sk, planning_area, effective_from, effective_to, is_current
    FROM msf_demo.gold.dim_client VERSION AS OF {BATCH1_VERSION}
    WHERE client_id = 'C0130'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## C0130 · 4. Why it matters: the point-in-time join
# MAGIC Each referral joins the client version valid **on the referral date**.
# MAGIC The three earlier referrals still count for Toa Payoh. Without SCD2, all five would count for Jurong West.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT f.referral_id, d.date AS referral_date, c.client_sk, c.planning_area AS area_at_the_time, c.is_current
# MAGIC FROM msf_demo.gold.fact_referral f
# MAGIC JOIN msf_demo.gold.dim_date   d ON d.date_sk   = f.date_sk
# MAGIC JOIN msf_demo.gold.dim_client c ON c.client_sk = f.client_sk
# MAGIC WHERE c.client_id = 'C0130'
# MAGIC ORDER BY referral_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## C0025 · 1. Rejected, and everything it blocked
# MAGIC Batch 1: postal code `12345` fails CLI_002. The original row is kept as JSON.
# MAGIC Its referrals fail REF_003, because a referral only passes if its client is **valid** (the cascade). All are RESOLVED now.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, business_key, failed_rules, status,
# MAGIC        get_json_object(record_json, '$.postal_code') AS original_postal,
# MAGIC        get_json_object(record_json, '$.client_id')   AS client_id,
# MAGIC        quarantined_at, resolved_at
# MAGIC FROM msf_demo.ops.quarantine
# MAGIC WHERE business_key = 'C0025' OR get_json_object(record_json, '$.client_id') = 'C0025'
# MAGIC ORDER BY quarantined_at

# COMMAND ----------

# MAGIC %md
# MAGIC ## C0025 · 2. The fix, and the audit trail
# MAGIC Batch 3: the source sent `clients, C0025, postal_code, 382038`. Remediation rebuilt the rejected row with the fix and **appended it to bronze**.
# MAGIC Silver then checked it with the same rules as any other row. The bad row and its fix both stay in bronze.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT client_id, postal_code, _source_file, _ingest_ts
# MAGIC FROM msf_demo.bronze.brz_clients
# MAGIC WHERE client_id = 'C0025'
# MAGIC ORDER BY _ingest_ts

# COMMAND ----------

# MAGIC %md
# MAGIC ## C0025 · 3. The referrals came back on their own
# MAGIC None of these had a correction. They were released because their client was fixed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT f.referral_id, d.date AS referral_date, c.planning_area
# MAGIC FROM msf_demo.gold.fact_referral f
# MAGIC JOIN msf_demo.gold.dim_date   d ON d.date_sk   = f.date_sk
# MAGIC JOIN msf_demo.gold.dim_client c ON c.client_sk = f.client_sk
# MAGIC WHERE c.client_id = 'C0025'
# MAGIC ORDER BY referral_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## The whole pipeline: quarantine outcome and DQ trend
# MAGIC 12 RESOLVED (8 corrected, 4 released with their client), 4 OPEN (no key, so they need a fix in the source system).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, status, count(*) AS rows
# MAGIC FROM msf_demo.ops.quarantine
# MAGIC GROUP BY table_name, status
# MAGIC ORDER BY table_name, status

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT r.batch_label, d.rule_id, d.rows_failed
# MAGIC FROM msf_demo.ops.dq_result d
# MAGIC JOIN (SELECT DISTINCT run_id, batch_label FROM msf_demo.ops.batch_run_log) r ON r.run_id = d.run_id
# MAGIC WHERE d.rule_id IN ('CLI_002', 'REF_002', 'REF_003') AND r.batch_label LIKE 'batch%'
# MAGIC ORDER BY d.rule_id, d.checked_at

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC | Question | Answer shown |
# MAGIC |---|---|
# MAGIC | Can I trust the data? | C0025 rejected with a reason; DQ failures fall to 0 across batches |
# MAGIC | Is personal data protected? | hashed and encrypted in silver; masks in `50_governance` |
# MAGIC | Is history correct? | C0130's two versions, and referrals counted where the client lived at the time |
# MAGIC | Can mistakes be fixed? | corrections replayed through bronze; 12 resolved, 4 visible for a source fix |
