# Databricks notebook source
# MAGIC %md
# MAGIC # 40_remediation
# MAGIC
# MAGIC Fixes quarantined rows using the corrections file sent by the source, **without editing any table by hand**.
# MAGIC
# MAGIC | Step | Where | What happens |
# MAGIC |---|---|---|
# MAGIC | 1. Match | this notebook | each correction is matched to an OPEN quarantine row by table and business key |
# MAGIC | 2. Replay | this notebook | the original rejected row is rebuilt with the corrected value and **appended to bronze**, tagged as a remediation row. Quarantine status becomes REPLAYED. |
# MAGIC | 3. Re-check | `21` / `22` silver | silver runs as normal. The corrected row is the newest version, so it's checked against the **same rules** as any other row. |
# MAGIC | 4. Release | `21` / `22` silver | any quarantined key that now made it into silver becomes RESOLVED. That includes rows with no correction of their own, e.g. referrals that were only blocked by their client. |
# MAGIC
# MAGIC Why go back through bronze instead of fixing silver directly:
# MAGIC - The fix passes the same quality rules as everything else. A wrong correction gets quarantined again.
# MAGIC - Bronze stays append-only. The original bad row is still there, next to its correction, for audit.
# MAGIC - Silver and gold can always be rebuilt from bronze, including the fixes.
# MAGIC
# MAGIC **Run order for batch 3:** `10_bronze` (loads the corrections file) → **this notebook** → `21` → `22` → `30` → `31`.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

CORRECTIONS = f"{CATALOG}.bronze.brz_corrections"
QUARANTINE = f"{CATALOG}.ops.quarantine"

# target_table in the corrections file -> (name used in quarantine and dq_rule, bronze table to append to)
TARGETS = {
    "clients":   ("silver.slv_clients",   f"{CATALOG}.bronze.brz_clients"),
    "referrals": ("silver.slv_referrals", f"{CATALOG}.bronze.brz_referrals"),
}

if not spark.catalog.tableExists(CORRECTIONS):
    dbutils.notebook.exit("No corrections loaded yet. Upload the corrections file and run 10_bronze first.")

corrections = spark.sql(f"""
    SELECT trim(target_table) AS target_table, trim(business_key) AS business_key,
           trim(column_name) AS column_name, trim(corrected_value) AS corrected_value, _source_file
    FROM {CORRECTIONS}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Before: which corrections match an open quarantine row?
# MAGIC A correction for a key that isn't in quarantine is **not** applied. That change should come from the source
# MAGIC as a normal update, not through remediation. Expect all 8 to match.

# COMMAND ----------

corrections.createOrReplaceTempView("corrections")
display(spark.sql(f"""
    SELECT c.target_table, c.business_key, c.column_name, c.corrected_value,
           q.status AS quarantine_status, q.failed_rules,
           get_json_object(q.record_json, concat('$.', c.column_name)) AS original_value
    FROM corrections c
    LEFT JOIN {QUARANTINE} q
      ON q.table_name = CASE c.target_table WHEN 'clients' THEN 'silver.slv_clients' ELSE 'silver.slv_referrals' END
     AND q.business_key = c.business_key
     AND q.status = 'OPEN'
    ORDER BY c.target_table, c.business_key
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Replay corrected rows into bronze
# MAGIC For each table:
# MAGIC 1. Collect the corrections per key into a map, e.g. `{postal_code: 382038}`. One key can have several corrections.
# MAGIC 2. Turn the quarantined row's JSON back into columns with `from_json`, using the bronze table's own schema.
# MAGIC 3. For each column, take the corrected value if there is one, otherwise the original.
# MAGIC 4. Stamp new lineage: `_source_file` says it came from remediation, with this run's id and time.
# MAGIC 5. Append to bronze, then mark the quarantine rows REPLAYED, so a re-run doesn't replay them twice.

# COMMAND ----------

for target_table, (quarantine_name, bronze_table) in TARGETS.items():
    log_start("remediation", bronze_table)
    try:
        schema = spark.table(bronze_table).schema
        source_cols = [f.name for f in schema.fields if not f.name.startswith("_")]

        mine = corrections.where(F.col("target_table") == target_table)

        # A correction naming a column the table doesn't have can't be applied. Report it, don't guess.
        unknown = [r.column_name for r in mine.where(~F.col("column_name").isin(source_cols))
                   .select("column_name").distinct().collect()]
        if unknown:
            print(f"WARNING {target_table}: corrections for unknown columns ignored: {unknown}")

        fixes = (mine.where(F.col("column_name").isin(source_cols))
                 .groupBy("business_key")
                 .agg(F.map_from_entries(F.collect_list(F.struct("column_name", "corrected_value"))).alias("fixes"),
                      F.first("_source_file").alias("corrections_file")))

        to_fix = (spark.table(QUARANTINE)
                  .where((F.col("table_name") == quarantine_name) & (F.col("status") == "OPEN"))
                  .join(fixes, "business_key")
                  .withColumn("r", F.from_json("record_json", schema)))

        # Rebuild every bronze column, in the table's own order
        cols = []
        for name in schema.fieldNames():
            if name in source_cols:
                cols.append(F.coalesce(F.col("fixes")[name], F.col(f"r.{name}")).alias(name))
            elif name == "_source_file":
                cols.append(F.concat(F.lit("remediation: "), F.col("corrections_file")).alias(name))
            elif name == "_ingest_ts":
                cols.append(F.current_timestamp().alias(name))
            elif name == "_run_id":
                cols.append(F.lit(RUN_ID).alias(name))
            else:
                cols.append(F.col(f"r.{name}").alias(name))       # e.g. _rescued_data, kept as it was

        # Remember exactly which quarantine rows are being replayed (a handful, so collecting is fine).
        # Captured BEFORE writing, so the list can't change between the append and the status update.
        replayed_rows = to_fix.select("record_json").collect()
        replayed = len(replayed_rows)
        if replayed:
            to_fix.select(cols).write.mode("append").saveAsTable(bronze_table)

            # Mark exactly those quarantine rows as REPLAYED (matched on the original row)
            spark.createDataFrame(replayed_rows, "record_json string").createOrReplaceTempView("replayed")
            spark.sql(f"""
                MERGE INTO {QUARANTINE} t
                USING replayed s
                ON t.table_name = '{quarantine_name}' AND t.record_json = s.record_json AND t.status = 'OPEN'
                WHEN MATCHED THEN UPDATE SET status = 'REPLAYED', resolved_run_id = '{RUN_ID}'
            """)

        note = f"replayed={replayed} corrections={mine.count()} unknown_columns={len(unknown)}"
        log_end(bronze_table, "SUCCESS", rows_in=mine.count(), rows_out=replayed, note=note)
        print(f"{target_table}: {note}")
    except Exception as e:
        log_end(bronze_table, "FAILED", error_msg=str(e)[:1000])
        raise

print("\nNext: run 21_silver_clients, then 22_silver_referrals (then 30 and 31), with batch_label = batch3.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## After running silver: what was fixed, and what is left
# MAGIC Run these cells **after** `21` and `22`. Expect 12 RESOLVED (8 corrected + 4 released with their client) and 4 OPEN.
# MAGIC The 4 OPEN rows have no usable key (or belong to the client with no key), so they can only be fixed in the source system.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, status, count(*) AS rows
# MAGIC FROM msf_demo.ops.quarantine
# MAGIC GROUP BY table_name, status
# MAGIC ORDER BY table_name, status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Every quarantined row and how it ended. 'released with client' = fixed without a correction of its own.
# MAGIC SELECT q.table_name, q.business_key, q.failed_rules, q.status,
# MAGIC        CASE WHEN q.status = 'RESOLVED' AND c.business_key IS NOT NULL THEN 'corrected'
# MAGIC             WHEN q.status = 'RESOLVED'                                THEN 'released with client'
# MAGIC             ELSE 'needs source fix' END                                AS outcome,
# MAGIC        q.quarantined_at, q.resolved_at
# MAGIC FROM msf_demo.ops.quarantine q
# MAGIC LEFT JOIN (SELECT DISTINCT trim(business_key) AS business_key FROM msf_demo.bronze.brz_corrections) c
# MAGIC   ON c.business_key = q.business_key
# MAGIC ORDER BY q.table_name, outcome, q.business_key

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Audit trail: the original bad row and its correction both stay in bronze.
# MAGIC SELECT client_id, postal_code, _source_file, _ingest_ts
# MAGIC FROM msf_demo.bronze.brz_clients
# MAGIC WHERE client_id IN ('C0025', 'C0060')
# MAGIC ORDER BY client_id, _ingest_ts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DQ failures per rule, run by run. The last referral run should show REF_002 = 0 after the dates were corrected.
# MAGIC SELECT r.batch_label, d.rule_id, d.rows_failed, d.checked_at
# MAGIC FROM msf_demo.ops.dq_result d
# MAGIC JOIN (SELECT DISTINCT run_id, batch_label FROM msf_demo.ops.batch_run_log) r ON r.run_id = d.run_id
# MAGIC WHERE d.rule_id IN ('CLI_002', 'REF_002', 'REF_003')
# MAGIC ORDER BY d.rule_id, d.checked_at
