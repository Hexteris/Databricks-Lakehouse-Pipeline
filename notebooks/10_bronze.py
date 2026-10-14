# Databricks notebook source
# MAGIC %md
# MAGIC # 10_bronze
# MAGIC
# MAGIC Bronze copies every new raw file into a Delta table, **unchanged**.
# MAGIC
# MAGIC - Every column is read as text. Bronze never guesses types, so nothing is lost or mis-converted (e.g. leading zeros in phone numbers).
# MAGIC - Each row gets three extra columns: which file it came from, when it was loaded, and which run loaded it.
# MAGIC - Auto Loader keeps a **checkpoint**: a record of which files it has already loaded.
# MAGIC   Re-running this notebook loads only new files, so it is safe to run any number of times.
# MAGIC
# MAGIC Cleaning and quality checks are silver's job, not bronze's.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

# MAGIC %md
# MAGIC ## The load
# MAGIC One function, called once per source below. It does the same four things for every source:
# MAGIC 1. Read new files from the source's landing folder with Auto Loader.
# MAGIC 2. Add the lineage columns.
# MAGIC 3. Append to the bronze table, process all new files, then stop.
# MAGIC 4. Count the rows this run added, and log the result.

# COMMAND ----------

def load_to_bronze(source, table):
    path = f"{LANDING}/{source}"
    target = f"{CATALOG}.bronze.{table}"

    # Auto Loader can't work out a schema from an empty folder, so skip sources that haven't arrived yet
    # (e.g. corrections only arrive in batch 3).
    if not [f for f in dbutils.fs.ls(path) if not f.isDir()]:
        print(f"{source}: no files yet, skipped")
        return

    log_start("bronze", target)
    try:
        df = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("header", "true")
              # Everything as text. Silver decides the real types.
              .option("cloudFiles.inferColumnTypes", "false")
              # Where Auto Loader stores the columns it has seen. New columns get added here (schema evolution).
              .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/{source}/schema")
              .load(path)
              # Lineage: where each row came from, when, and which run brought it in.
              .withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("_run_id", F.lit(RUN_ID)))

        (df.writeStream
           # The checkpoint is Auto Loader's memory of which files it has loaded. One per source.
           .option("checkpointLocation", f"{CHECKPOINTS}/{source}/checkpoint")
           # Allow new columns into the table. Session-wide auto-merge is blocked on Free Edition (smoke test).
           .option("mergeSchema", "true")
           # Process everything new, then stop. Batch-style cost, with streaming's file tracking.
           .trigger(availableNow=True)
           .toTable(target)
           .awaitTermination())

        rows = spark.table(target).where(F.col("_run_id") == RUN_ID).count()
        log_end(target, "SUCCESS", rows_out=rows)
        print(f"{source}: {rows} new rows -> {target}")
    except Exception as e:
        log_end(target, "FAILED", error_msg=str(e)[:1000])
        raise   # re-raise, so a Job sees the failure and can retry

# COMMAND ----------

load_to_bronze("programmes",  "brz_programmes")
load_to_bronze("clients",     "brz_clients")
load_to_bronze("referrals",   "brz_referrals")
load_to_bronze("corrections", "brz_corrections")
load_to_bronze("population",  "brz_population")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check
# MAGIC Rows per table and per file. A re-run with no new files should add nothing.

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name, status, rows_out, started_at, ended_at, error_msg
    FROM {CATALOG}.ops.batch_run_log
    WHERE run_id = '{RUN_ID}'
    ORDER BY table_name"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which file each table's rows came from. This replaces a separate file-manifest table.
# MAGIC SELECT 'brz_clients' AS tbl, _source_file, count(*) AS rows FROM msf_demo.bronze.brz_clients GROUP BY ALL
# MAGIC UNION ALL
# MAGIC SELECT 'brz_referrals', _source_file, count(*) FROM msf_demo.bronze.brz_referrals GROUP BY ALL
# MAGIC UNION ALL
# MAGIC SELECT 'brz_programmes', _source_file, count(*) FROM msf_demo.bronze.brz_programmes GROUP BY ALL
# MAGIC UNION ALL
# MAGIC SELECT 'brz_population', _source_file, count(*) FROM msf_demo.bronze.brz_population GROUP BY ALL
# MAGIC ORDER BY tbl, _source_file
