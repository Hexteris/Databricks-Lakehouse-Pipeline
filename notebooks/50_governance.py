# Databricks notebook source
# MAGIC %md
# MAGIC # 50_governance
# MAGIC
# MAGIC Silver already **transformed** the PII: NRIC is hashed, phone and email are encrypted.
# MAGIC This notebook adds the second layer: **who sees what**, decided at query time by Unity Catalog.
# MAGIC
# MAGIC | | Hashing / encryption (silver) | Column mask (this notebook) |
# MAGIC |---|---|---|
# MAGIC | Changes the stored data? | yes | no |
# MAGIC | Depends on who is asking? | no | **yes** |
# MAGIC | Can be bypassed by querying another view? | n/a | **no**: the rule is attached to the column itself |
# MAGIC
# MAGIC Masks go on `gold.dim_client`, the only gold table with personal data. `obt_referral_wide` has no personal data,
# MAGIC so it needs no mask: the safest PII is the PII you never copy.
# MAGIC
# MAGIC **Run this last**, after `30` and `31`. A reset drops gold, so re-run this after every rebuild.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

TABLE = f"{CATALOG}.gold.dim_client"
ME = spark.sql("SELECT current_user()").first()[0]

# Real rule: members of the pii_readers group see real values.
# Demo switch: set this to 'yes' to add yourself to the rule, so the effect can be shown live without changing groups.
dbutils.widgets.dropdown("pii_access_for_me", "no", ["no", "yes"], "Show PII to me?")
ALLOWED = "is_account_group_member('pii_readers')"
if dbutils.widgets.get("pii_access_for_me") == "yes":
    ALLOWED += f" OR current_user() = '{ME}'"
print("Unmasked for:", ALLOWED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mask functions
# MAGIC Each one returns the real value for allowed users, and a reduced value for everyone else.
# MAGIC
# MAGIC | Function | Used on | Everyone else sees | Why |
# MAGIC |---|---|---|---|
# MAGIC | `mask_name` | full_name | `T***` | a name identifies a person directly |
# MAGIC | `mask_postal` | postal_code | `31****` | the sector (area) is enough for analysis, the full code points at a block |
# MAGIC | `mask_hidden` | nric_hash, phone_enc, email_enc | `[hidden]` | even a hash or ciphertext lets someone link records, so analysts don't need it |
# MAGIC
# MAGIC `nric_masked` (`FAKE-***1234`) is already safe to show, so it isn't masked again.
# MAGIC The table points to each function by name, so redefining a function changes every query at once. No re-attaching needed.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.gold.mask_name(v STRING) RETURNS STRING
RETURN CASE WHEN {ALLOWED} THEN v ELSE concat(left(v, 1), '***') END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.gold.mask_postal(v STRING) RETURNS STRING
RETURN CASE WHEN {ALLOWED} THEN v ELSE concat(left(v, 2), '****') END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.gold.mask_hidden(v STRING) RETURNS STRING
RETURN CASE WHEN {ALLOWED} THEN v ELSE '[hidden]' END
""")
print("Mask functions ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attach the masks to the columns

# COMMAND ----------

masks = {
    "full_name":   "mask_name",
    "postal_code": "mask_postal",
    "nric_hash":   "mask_hidden",
    "phone_enc":   "mask_hidden",
    "email_enc":   "mask_hidden",
}
for column, fn in masks.items():
    spark.sql(f"ALTER TABLE {TABLE} ALTER COLUMN {column} SET MASK {CATALOG}.gold.{fn}")
    print(f"{column} -> {fn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check: same query, different result depending on who asks
# MAGIC With the switch on **no**: masked. Set it to **yes**, re-run the function cell (not the attach cell) and this one: real values.
# MAGIC The query never changes. That's the point.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT client_id, full_name, nric_masked, nric_hash, phone_enc, postal_code, planning_area, is_current
# MAGIC FROM msf_demo.gold.dim_client
# MAGIC WHERE client_id = 'C0130'
# MAGIC ORDER BY effective_from

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analysis still works: the columns analysts need (area, household type) are not masked.
# MAGIC SELECT c.planning_area, count(*) AS referrals
# MAGIC FROM msf_demo.gold.fact_referral f
# MAGIC JOIN msf_demo.gold.dim_client c ON c.client_sk = f.client_sk
# MAGIC GROUP BY c.planning_area
# MAGIC ORDER BY referrals DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which columns carry a mask, and which function. If this view isn't available, Catalog Explorer shows masks per column.
# MAGIC SELECT table_name, column_name, mask_name
# MAGIC FROM msf_demo.information_schema.column_masks
# MAGIC WHERE table_schema = 'gold'

# COMMAND ----------

# MAGIC %md
# MAGIC ## In production
# MAGIC - Access by group, never by named user: `pii_readers` for case officers, `analysts` with `SELECT` on `gold` only.
# MAGIC   Bronze (raw PII) readable only by the pipeline's service principal.
# MAGIC - A **row filter** could further limit each officer to their own office's clients, using the same function pattern.
# MAGIC
# MAGIC **Only if needed:** if `30_gold_dimensions` ever fails to MERGE into the masked table, remove the masks with
# MAGIC `ALTER TABLE msf_demo.gold.dim_client ALTER COLUMN <column> DROP MASK`, re-run `30`, then re-run this notebook.
