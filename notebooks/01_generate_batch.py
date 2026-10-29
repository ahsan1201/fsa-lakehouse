# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Drop one day of source extracts into the landing Volume
# MAGIC
# MAGIC Plays the role of the legacy FSA system's nightly export. Run it with `batch_day` = 1, 2, 3...
# MAGIC Day 1 also writes the program reference file and a full producer snapshot.
# MAGIC Day 3 contains a corrupt JSON line. From day 4, a new column (`disbursement_method`) appears.
# MAGIC
# MAGIC After running, browse **Catalog → fsa → landing → raw** to see the files.

# COMMAND ----------

dbutils.widgets.text("batch_day", "1", "Batch day (1, 2, 3...)")
dbutils.widgets.text("n_payments", "5000", "Payments per day")

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../src"))

from fsa_lakehouse import config as C
from fsa_lakehouse.generator import generate_batch

batch_day = int(dbutils.widgets.get("batch_day"))
result = generate_batch(C.LANDING_ROOT, batch_day, n_payments=int(dbutils.widgets.get("n_payments")))
result

# COMMAND ----------

display(dbutils.fs.ls(f"{C.PAYMENTS_LANDING}/{result['batch_date']}"))
