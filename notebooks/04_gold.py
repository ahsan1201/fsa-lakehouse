# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold: business tables + Delta performance and time travel
# MAGIC
# MAGIC **What you're learning**
# MAGIC * Gold tables answer specific business questions; here, *"how much did each program pay, by state
# MAGIC   and year?"* and *"which producers exceeded the $125K ARC/PLC payment limit?"*
# MAGIC * **Liquid clustering** (`clusterBy`) replaces manual partitioning + Z-order.
# MAGIC * `OPTIMIZE`, `DESCRIBE HISTORY`, `DESCRIBE DETAIL`, **time travel** (`VERSION AS OF`) and `RESTORE`.
# MAGIC
# MAGIC Full recompute is fine at this size. Talking point for the interview: at USDA scale you'd switch
# MAGIC to materialized views (see `pipelines/`) or incremental MERGE driven by Change Data Feed.

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../src"))

from fsa_lakehouse import config as C
from fsa_lakehouse import transforms as T

payments = spark.table(C.SILVER_PAYMENTS)
programs = spark.table(C.SILVER_PROGRAMS)
producers_current = spark.table(C.SILVER_PRODUCERS).filter("is_current")

# COMMAND ----------

(T.payments_by_state_program_year(payments, programs)
 .write.mode("overwrite").option("overwriteSchema", "true")
 .clusterBy("program_year", "state_code")
 .saveAsTable(C.GOLD_STATE_PROGRAM))

(T.producer_payment_limit_check(payments, producers_current, programs)
 .write.mode("overwrite").option("overwriteSchema", "true")
 .clusterBy("program_year", "needs_review")
 .saveAsTable(C.GOLD_PRODUCER_LIMITS))

# COMMAND ----------

display(spark.sql(f"""
SELECT program_year, program_category, program_code,
       sum(payment_count) AS payments, round(sum(total_paid), 0) AS total_paid
FROM {C.GOLD_STATE_PROGRAM}
GROUP BY ALL ORDER BY total_paid DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT * FROM {C.GOLD_PRODUCER_LIMITS}
WHERE needs_review ORDER BY arc_plc_net_paid DESC LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md ### Delta internals: run each cell and read the output carefully

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {C.SILVER_PAYMENTS}"))   # every MERGE is a version; check operationMetrics

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {C.SILVER_PAYMENTS}"))    # numFiles, sizeInBytes, clusteringColumns

# COMMAND ----------

display(spark.sql(f"OPTIMIZE {C.SILVER_PAYMENTS}"))           # compacts small files, applies clustering

# COMMAND ----------

# Time travel: how many payments existed at version 1 vs now?
v1 = spark.sql(f"SELECT count(*) c FROM {C.SILVER_PAYMENTS} VERSION AS OF 1").first().c
now = spark.table(C.SILVER_PAYMENTS).count()
print(f"version 1: {v1:,} rows   current: {now:,} rows")
