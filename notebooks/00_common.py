# Databricks notebook source
# MAGIC %md
# MAGIC # 00_common
# MAGIC
# MAGIC Shared settings and run-log helpers. Every pipeline notebook starts with `%run ./00_common`,
# MAGIC so names and logging are defined in one place instead of copied into each notebook.
# MAGIC This notebook doesn't do anything on its own.

# COMMAND ----------

import uuid
from pyspark.sql import functions as F

CATALOG = "msf_demo"
SECRET_SCOPE = "msf_demo"
LANDING = f"/Volumes/{CATALOG}/landing/raw_files"
CHECKPOINTS = f"/Volumes/{CATALOG}/ops/checkpoints"

# Which batch is being processed. Only used for the run log, so it reads well later.
dbutils.widgets.text("batch_label", "batch1", "Batch label")
BATCH_LABEL = dbutils.widgets.get("batch_label")

# A new id each time a notebook runs. Rows written by the run carry it, so one load can be traced or removed.
RUN_ID = str(uuid.uuid4())
print("run_id:", RUN_ID, "| batch:", BATCH_LABEL)

# COMMAND ----------

# A row is written as STARTED before the work, then updated to SUCCESS or FAILED.
# If the notebook crashes mid-way, the row stays STARTED, so the failure is visible rather than silent.

def log_start(step, table_name):
    spark.sql(f"""
        INSERT INTO {CATALOG}.ops.batch_run_log (run_id, batch_label, step, table_name, status, started_at)
        VALUES (:run_id, :batch, :step, :tbl, 'STARTED', current_timestamp())""",
        args={"run_id": RUN_ID, "batch": BATCH_LABEL, "step": step, "tbl": table_name})


def log_end(table_name, status, rows_in=None, rows_out=None, rows_rejected=None, note=None, error_msg=None):
    spark.sql(f"""
        UPDATE {CATALOG}.ops.batch_run_log
        SET status = :status, ended_at = current_timestamp(),
            rows_in = :rows_in, rows_out = :rows_out, rows_rejected = :rows_rej, note = :note, error_msg = :err
        WHERE run_id = :run_id AND table_name = :tbl""",
        args={"status": status, "rows_in": rows_in, "rows_out": rows_out, "rows_rej": rows_rejected,
              "note": note, "err": error_msg, "run_id": RUN_ID, "tbl": table_name})
