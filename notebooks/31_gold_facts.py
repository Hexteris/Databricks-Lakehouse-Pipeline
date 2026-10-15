# Databricks notebook source
# MAGIC %md
# MAGIC # 31_gold_facts
# MAGIC
# MAGIC Builds the centre of the star schema, plus a flattened copy of it.
# MAGIC
# MAGIC | Table | Shape | Who it's for |
# MAGIC |---|---|---|
# MAGIC | `fact_referral` | **Star**: one row per referral, with keys pointing to each dimension | analysts writing SQL, BI models |
# MAGIC | `obt_referral_wide` | **Wide** ("one big table"): fact and dimensions joined in advance | dashboards and ML, no joins needed |
# MAGIC
# MAGIC **Grain: one row = one referral.** Every measure and every count is at that level.
# MAGIC
# MAGIC Both tables are **rebuilt on every run** from silver and the dimensions. At this size that's fast,
# MAGIC and it keeps them simple and always consistent: a referral released from quarantine, or a new client version,
# MAGIC is reflected automatically. At production scale I'd switch the fact to an incremental MERGE.
# MAGIC
# MAGIC Run after `30_gold_dimensions`.
# MAGIC
# MAGIC *Programme and agency names are illustrative and do not represent real MSF schemes or data.*

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_referral and the point-in-time join
# MAGIC
# MAGIC Each referral is linked to the client **version that was valid on the referral date**:
# MAGIC
# MAGIC `referral_date >= effective_from AND referral_date < effective_to`
# MAGIC
# MAGIC The location also comes from **that version's** planning area, not the client's current one.
# MAGIC So if a client moves from Toa Payoh to Jurong West, their earlier referrals still count for Toa Payoh.
# MAGIC
# MAGIC Joins are LEFT JOINs with `-1` for "not found", so a missing dimension row can never silently drop a referral.
# MAGIC The checks below expect zero `-1` keys.

# COMMAND ----------

target = f"{CATALOG}.gold.fact_referral"
log_start("gold", target)
try:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {target} AS
    SELECT
        r.referral_id,                                               -- the source id, kept for tracing
        coalesce(d.date_sk,      -1)          AS date_sk,
        coalesce(c.client_sk,    -1)          AS client_sk,
        coalesce(p.programme_sk, -1)          AS programme_sk,
        coalesce(l.location_sk,  -1)          AS location_sk,
        r.status,
        CASE WHEN r.status = 'Approved' THEN 1 ELSE 0 END AS is_approved,
        r._silver_run_id,
        current_timestamp()                   AS _gold_built_at
    FROM {CATALOG}.silver.slv_referrals r
    LEFT JOIN {CATALOG}.gold.dim_date d
           ON d.date = r.referral_date
    LEFT JOIN {CATALOG}.gold.dim_client c                            -- the version valid on the referral date
           ON c.client_id = r.client_id
          AND r.referral_date >= c.effective_from
          AND r.referral_date <  c.effective_to
    LEFT JOIN {CATALOG}.gold.dim_programme p
           ON p.programme_id = r.programme_id
    LEFT JOIN {CATALOG}.gold.dim_location l                          -- that version's planning area
           ON lower(l.planning_area) = lower(c.planning_area)
    """)
    rows_in = spark.table(f"{CATALOG}.silver.slv_referrals").count()
    rows_out = spark.table(target).count()
    log_end(target, "SUCCESS", rows_in=rows_in, rows_out=rows_out)
    print(f"{target}: {rows_in} silver referrals -> {rows_out} fact rows")
except Exception as e:
    log_end(target, "FAILED", error_msg=str(e)[:1000])
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## obt_referral_wide
# MAGIC The same rows with every useful dimension column copied in. Faster and simpler to query, at the cost of
# MAGIC repeating values (e.g. the programme name on every referral) and needing a rebuild whenever a dimension changes.
# MAGIC
# MAGIC **No personal data goes in**, not even hashed or encrypted. This table is for broad reporting and ML,
# MAGIC so it only gets what those uses need. Personal data stays in `dim_client`, where access can be controlled.

# COMMAND ----------

target = f"{CATALOG}.gold.obt_referral_wide"
log_start("gold", target)
try:
    spark.sql(f"""
    CREATE OR REPLACE TABLE {target} AS
    SELECT
        f.referral_id,
        f.status,
        f.is_approved,
        d.date            AS referral_date,
        d.year, d.quarter, d.month, d.month_name, d.day_name, d.is_weekend,
        p.programme_id, p.programme_name, p.agency_name,
        c.client_id,
        c.household_type,
        l.planning_area,
        l.total_residents,
        l.pct_65_plus,
        current_timestamp() AS _gold_built_at
    FROM {CATALOG}.gold.fact_referral f
    LEFT JOIN {CATALOG}.gold.dim_date      d ON d.date_sk      = f.date_sk
    LEFT JOIN {CATALOG}.gold.dim_programme p ON p.programme_sk = f.programme_sk
    LEFT JOIN {CATALOG}.gold.dim_client    c ON c.client_sk    = f.client_sk
    LEFT JOIN {CATALOG}.gold.dim_location  l ON l.location_sk  = f.location_sk
    """)
    rows_out = spark.table(target).count()
    log_end(target, "SUCCESS", rows_in=spark.table(f"{CATALOG}.gold.fact_referral").count(), rows_out=rows_out)
    print(f"{target}: rows={rows_out}")
except Exception as e:
    log_end(target, "FAILED", error_msg=str(e)[:1000])
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Nothing lost on the way to gold, and every key found its dimension row.
# MAGIC -- Batch 1: 392 / 392 / 392, and 0 for every unknown_* column.
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM msf_demo.silver.slv_referrals)   AS silver_referrals,
# MAGIC   (SELECT count(*) FROM msf_demo.gold.fact_referral)     AS fact_rows,
# MAGIC   (SELECT count(*) FROM msf_demo.gold.obt_referral_wide) AS wide_rows,
# MAGIC   count_if(date_sk = -1)      AS unknown_date,
# MAGIC   count_if(client_sk = -1)    AS unknown_client,
# MAGIC   count_if(programme_sk = -1) AS unknown_programme,
# MAGIC   count_if(location_sk = -1)  AS unknown_location
# MAGIC FROM msf_demo.gold.fact_referral

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Point-in-time check: every fact row points at a client version that was valid on the referral date. Expect 0.
# MAGIC SELECT count(*) AS wrong_version
# MAGIC FROM msf_demo.gold.fact_referral f
# MAGIC JOIN msf_demo.gold.dim_date   d ON d.date_sk   = f.date_sk
# MAGIC JOIN msf_demo.gold.dim_client c ON c.client_sk = f.client_sk
# MAGIC WHERE NOT (d.date >= c.effective_from AND d.date < c.effective_to)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Star vs wide: the same question two ways
# MAGIC **Which planning areas have the most referrals per 100,000 residents?**
# MAGIC Referral counts are synthetic, the resident counts are real Census 2020 figures.
# MAGIC
# MAGIC Note: the generator assigns clients to areas at random, so small areas will look "high need".
# MAGIC That's an artefact of the synthetic data. The query, not the ranking, is the point.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- STAR: join the fact to the dimension it needs. Flexible, and each fact is stored once.
# MAGIC SELECT l.planning_area,
# MAGIC        count(*)                                           AS referrals,
# MAGIC        l.total_residents,
# MAGIC        round(100000 * count(*) / l.total_residents, 1)    AS per_100k_residents,
# MAGIC        round(100 * avg(f.is_approved), 1)                 AS pct_approved
# MAGIC FROM msf_demo.gold.fact_referral f
# MAGIC JOIN msf_demo.gold.dim_location l ON l.location_sk = f.location_sk
# MAGIC GROUP BY l.planning_area, l.total_residents
# MAGIC ORDER BY per_100k_residents DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- WIDE: same answer, no join. Simpler for dashboard tools and ML, but the table repeats values and must be rebuilt
# MAGIC -- whenever a dimension changes.
# MAGIC SELECT planning_area,
# MAGIC        count(*)                                           AS referrals,
# MAGIC        total_residents,
# MAGIC        round(100000 * count(*) / total_residents, 1)      AS per_100k_residents,
# MAGIC        round(100 * avg(is_approved), 1)                   AS pct_approved
# MAGIC FROM msf_demo.gold.obt_referral_wide
# MAGIC GROUP BY planning_area, total_residents
# MAGIC ORDER BY per_100k_residents DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why SCD2 matters: one client's story
# MAGIC Client **C0130** lives in Toa Payoh in batch 1 and moves to Jurong West on 2026-07-01 in batch 2.
# MAGIC After batch 1: 3 referrals, all Toa Payoh. After batch 2: the same 3 still say Toa Payoh, plus 2 new ones in Jurong West.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT f.referral_id, d.date AS referral_date, c.client_sk, c.planning_area AS area_at_the_time,
# MAGIC        c.effective_from, c.effective_to, c.is_current
# MAGIC FROM msf_demo.gold.fact_referral f
# MAGIC JOIN msf_demo.gold.dim_date   d ON d.date_sk   = f.date_sk
# MAGIC JOIN msf_demo.gold.dim_client c ON c.client_sk = f.client_sk
# MAGIC WHERE c.client_id = 'C0130'
# MAGIC ORDER BY referral_date

# COMMAND ----------

# MAGIC %sql
# MAGIC -- "As it was" vs "as it is now": referrals by the area at the time vs the client's current area.
# MAGIC -- Batch 1: no client has moved, so 0 rows. After batch 2 this shows which areas would be miscounted without SCD2.
# MAGIC WITH at_time AS (
# MAGIC   SELECT c.planning_area, count(*) AS n FROM msf_demo.gold.fact_referral f
# MAGIC   JOIN msf_demo.gold.dim_client c ON c.client_sk = f.client_sk GROUP BY 1
# MAGIC ), now AS (
# MAGIC   SELECT cur.planning_area, count(*) AS n FROM msf_demo.gold.fact_referral f
# MAGIC   JOIN msf_demo.gold.dim_client c   ON c.client_sk = f.client_sk
# MAGIC   JOIN msf_demo.gold.dim_client cur ON cur.client_id = c.client_id AND cur.is_current
# MAGIC   GROUP BY 1
# MAGIC )
# MAGIC SELECT coalesce(a.planning_area, n.planning_area) AS planning_area,
# MAGIC        coalesce(a.n, 0) AS referrals_as_it_was,
# MAGIC        coalesce(n.n, 0) AS referrals_if_current_area_used,
# MAGIC        coalesce(n.n, 0) - coalesce(a.n, 0) AS difference
# MAGIC FROM at_time a FULL OUTER JOIN now n ON a.planning_area = n.planning_area
# MAGIC WHERE coalesce(a.n, 0) <> coalesce(n.n, 0)
# MAGIC ORDER BY abs(coalesce(n.n, 0) - coalesce(a.n, 0)) DESC

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name, status, rows_in, rows_out, error_msg
    FROM {CATALOG}.ops.batch_run_log WHERE run_id = '{RUN_ID}' ORDER BY table_name"""))
