# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Break it on purpose
# MAGIC
# MAGIC Each exercise is a production incident in miniature. For each one:
# MAGIC **predict** what will happen, **run** it, then **explain** what you saw out loud in two sentences,
# MAGIC as if answering an interviewer. Write your explanation in a markdown cell under the exercise.

# COMMAND ----------

import os, sys, shutil
sys.path.append(os.path.abspath("../src"))
from fsa_lakehouse import config as C

def counts():
    return {t: spark.table(t).count() for t in [C.BRONZE_PAYMENTS, C.SILVER_PAYMENTS, C.SILVER_PAYMENTS_QUARANTINE]}

before = counts(); before

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1 · Rerun with no new files
# MAGIC Run notebooks 02 and 03 again right now, without generating a new batch.
# MAGIC **Predict:** do any counts change? Then run the next cell.

# COMMAND ----------

after = counts()
{k: after[k] - before[k] for k in after}
# Expected: all zeros. Checkpoints record which files and offsets were processed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2 · The source system re-sends yesterday's file under a new name
# MAGIC Very common: an upstream job retries and re-exports. Auto Loader sees a *new* file name, so bronze
# MAGIC **will** grow. **Predict:** does silver grow too?

# COMMAND ----------

day_dirs = sorted(d.path for d in dbutils.fs.ls(C.PAYMENTS_LANDING))
src_dir = day_dirs[-1].replace("dbfs:", "")
src_file = sorted(f.path for f in dbutils.fs.ls(src_dir))[0].replace("dbfs:", "")
dst_file = src_file.replace(".json", "_RESENT.json")
shutil.copy(src_file, dst_file)
print("re-sent:", dst_file)
before = counts()

# COMMAND ----------

# MAGIC %md Now run 02 and 03 again, then:

# COMMAND ----------

after = counts()
{k: after[k] - before[k] for k in after}
# Expected: bronze +N, silver +0. MERGE on payment_id makes silver idempotent.
# Quarantine grows by the bad rows in that file. Would you dedupe quarantine too? Discuss.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3 · Someone runs a bad UPDATE in production
# MAGIC Recover with time travel. This is the "how do you recover from a bad deploy?" question.

# COMMAND ----------

spark.sql(f"UPDATE {C.SILVER_PAYMENTS} SET payment_amount = 0 WHERE state_code = 'IA'")
display(spark.sql(f"SELECT state_code, sum(payment_amount) FROM {C.SILVER_PAYMENTS} GROUP BY 1 ORDER BY 1"))

# COMMAND ----------

hist = spark.sql(f"DESCRIBE HISTORY {C.SILVER_PAYMENTS}").select("version", "operation").collect()
bad_version = [h.version for h in hist if h.operation == "UPDATE"][0]
print("restoring to version", bad_version - 1)
display(spark.sql(f"RESTORE TABLE {C.SILVER_PAYMENTS} TO VERSION AS OF {bad_version - 1}"))
display(spark.sql(f"SELECT state_code, sum(payment_amount) FROM {C.SILVER_PAYMENTS} GROUP BY 1 ORDER BY 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4 · Schema drift
# MAGIC Generate day 4 (01 with `batch_day=4`) if you haven't, then run 02. Find the message where the stream
# MAGIC restarted. Then check:

# COMMAND ----------

display(spark.sql(f"""
SELECT disbursement_method, count(*) FROM {C.SILVER_PAYMENTS} GROUP BY 1
"""))
# Days 1-3 are NULL and day 4+ are populated. Nothing broke, and no one edited a DDL by hand.
# Interview follow-up: what if the source *renamed* or *retyped* a column? (addNewColumns won't save you:
# the value goes to _rescued_data. Talk about schema hints, rescue mode, and alerting on _rescued_data.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5 · Reconciliation: did every record land somewhere?

# COMMAND ----------

display(spark.sql(f"""
SELECT entity, sum(rows_in) AS rows_in, sum(rows_valid) AS valid,
       sum(rows_quarantined) AS quarantined,
       sum(rows_in) - sum(rows_valid) - sum(rows_quarantined) AS unaccounted
FROM {C.OPS_RUN_AUDIT} GROUP BY entity
"""))
# `unaccounted` must be 0. This is the query a federal client's data steward will ask you for.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6 · Look at the query plan
# MAGIC Run this, open the Spark UI / query profile link under the cell, and find: the files pruned by clustering,
# MAGIC and whether the join was a broadcast join.

# COMMAND ----------

display(spark.sql(f"""
SELECT p.state_code, d.program_category, sum(p.payment_amount) total
FROM {C.SILVER_PAYMENTS} p JOIN {C.SILVER_PROGRAMS} d USING (program_code)
WHERE p.program_year = 2025 AND p.state_code = 'IA'
GROUP BY ALL
"""))
