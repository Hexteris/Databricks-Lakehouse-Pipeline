# Databricks notebook source
# MAGIC %md
# MAGIC # 20_silver_reference
# MAGIC
# MAGIC Turns the two reference sources into clean lookup tables:
# MAGIC
# MAGIC | Table | Source | What silver does |
# MAGIC |---|---|---|
# MAGIC | `slv_programmes` | synthetic | trim text, keep the latest version of each programme |
# MAGIC | `slv_population` | **real**, data.gov.sg Census 2020 | reshape a wide report into one row per planning area |
# MAGIC
# MAGIC Both tables are small and arrive as full snapshots, so they are **rebuilt from bronze on every run**
# MAGIC (`CREATE OR REPLACE`). That's simpler than a MERGE and still safe: Delta keeps the previous version in its history.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

def rebuild(source_table, target_table, select_sql):
    """Rebuild one silver table from bronze, and log rows in and out."""
    target = f"{CATALOG}.silver.{target_table}"
    log_start("silver", target)
    try:
        spark.sql(f"CREATE OR REPLACE TABLE {target} AS {select_sql}")
        rows_in = spark.table(f"{CATALOG}.bronze.{source_table}").count()
        rows_out = spark.table(target).count()
        log_end(target, "SUCCESS", rows_in=rows_in, rows_out=rows_out)
        print(f"{target}: {rows_in} bronze rows -> {rows_out} silver rows")
    except Exception as e:
        log_end(target, "FAILED", error_msg=str(e)[:1000])
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Programmes
# MAGIC Bronze is append-only, so if the programme list is sent again, bronze holds every copy.
# MAGIC `row_number()` keeps only the newest copy of each `programme_id`. This is the dedup pattern used for every source.

# COMMAND ----------

rebuild("brz_programmes", "slv_programmes", f"""
    SELECT programme_id, programme_name, agency_name, _source_file, _ingest_ts
    FROM (
        SELECT
            trim(programme_id)   AS programme_id,
            trim(programme_name) AS programme_name,
            trim(agency_name)    AS agency_name,
            _source_file,
            _ingest_ts,
            -- 1 = newest copy of this programme
            row_number() OVER (PARTITION BY trim(programme_id) ORDER BY _ingest_ts DESC) AS rn
        FROM {CATALOG}.bronze.brz_programmes
        WHERE nullif(trim(programme_id), '') IS NOT NULL
    )
    WHERE rn = 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Population (real data)
# MAGIC The census file is a report, not a table. It mixes three kinds of row:
# MAGIC
# MAGIC | Row | Example | Kept? |
# MAGIC |---|---|---|
# MAGIC | National total | `Total` | no |
# MAGIC | Planning area total | `Ang Mo Kio - Total` (one is written `Changi- Total`) | **yes**, renamed to `Ang Mo Kio` |
# MAGIC | Subzone | `Cheng San` | no, our clients only have a planning area |
# MAGIC
# MAGIC The name column is called `Number`, and the census uses `-` to mean zero.
# MAGIC Out of about 150 columns we keep total residents and residents aged 65+, which is what gold needs.
# MAGIC Any other unexpected value makes the `CAST` fail, so a changed file format stops the run instead of loading wrong numbers.

# COMMAND ----------

def census_int(col):
    # '-' means zero in census tables. Anything else must be a whole number.
    return f"CAST(CASE WHEN trim({col}) = '-' THEN '0' ELSE trim({col}) END AS INT)"

age_65_plus = ["Total1_65_69", "Total1_70_74", "Total1_75_79", "Total1_80_84", "Total1_85_89", "Total1_90andOver"]
sum_65_plus = " + ".join(census_int(c) for c in age_65_plus)

rebuild("brz_population", "slv_population", f"""
    SELECT
        planning_area,
        total_residents,
        residents_65_plus,
        round(100 * residents_65_plus / nullif(total_residents, 0), 1) AS pct_65_plus,
        'Census of Population 2020' AS data_source,
        _source_file,
        _ingest_ts
    FROM (
        SELECT
            -- '[ ]*-[ ]*' allows any spacing around the dash: the file has 'Changi- Total' as well as 'Ang Mo Kio - Total'
            trim(regexp_replace(Number, '[ ]*-[ ]*Total$', '')) AS planning_area,
            {census_int("Total1_Total")}                  AS total_residents,
            {sum_65_plus}                                 AS residents_65_plus,
            _source_file,
            _ingest_ts,
            row_number() OVER (PARTITION BY trim(Number) ORDER BY _ingest_ts DESC) AS rn
        FROM {CATALOG}.bronze.brz_population
        WHERE trim(Number) RLIKE '-[ ]*Total$'   -- planning-area rows only (the national row is just 'Total')
    )
    WHERE rn = 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Biggest planning areas. Sanity check against known figures (e.g. Bedok is one of the largest).
# MAGIC SELECT planning_area, total_residents, residents_65_plus, pct_65_plus
# MAGIC FROM msf_demo.silver.slv_population
# MAGIC ORDER BY total_residents DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Do the planning-area totals add up to the national total in the same file?
# MAGIC -- Census figures are rounded to the nearest 10, so each area can be off by up to 5.
# MAGIC -- A gap bigger than 5 x (number of areas) means a planning area was dropped, not rounded.
# MAGIC WITH t AS (
# MAGIC   SELECT
# MAGIC     (SELECT count(*)             FROM msf_demo.silver.slv_population) AS areas,
# MAGIC     (SELECT sum(total_residents) FROM msf_demo.silver.slv_population) AS sum_of_areas,
# MAGIC     (SELECT CAST(Total1_Total AS INT) FROM msf_demo.bronze.brz_population
# MAGIC      WHERE trim(Number) = 'Total' LIMIT 1)                            AS national_total
# MAGIC )
# MAGIC SELECT *, national_total - sum_of_areas AS difference,
# MAGIC        abs(national_total - sum_of_areas) <= 5 * areas AS within_rounding
# MAGIC FROM t

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Every planning area used by clients must exist in the population table, or the gold join will drop it.
# MAGIC -- Expect 0 rows.
# MAGIC SELECT DISTINCT trim(c.planning_area) AS missing_planning_area
# MAGIC FROM msf_demo.bronze.brz_clients c
# MAGIC LEFT ANTI JOIN msf_demo.silver.slv_population p
# MAGIC   ON lower(trim(c.planning_area)) = lower(p.planning_area)

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name, status, rows_in, rows_out, error_msg
    FROM {CATALOG}.ops.batch_run_log WHERE run_id = '{RUN_ID}' ORDER BY table_name"""))
