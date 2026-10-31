# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver: clean, validate, deduplicate, upsert (MERGE), SCD Type 2
# MAGIC
# MAGIC **What you're learning**
# MAGIC * Streaming **from a Delta table** (bronze) with its own checkpoint, so only new bronze rows are processed.
# MAGIC * `foreachBatch` + `MERGE`: the standard pattern for idempotent upserts. Replaying the same data
# MAGIC   doesn't create duplicates.
# MAGIC * **Quarantine instead of drop**: bad rows go to a table with the reason attached, and nothing silently disappears.
# MAGIC * **SCD Type 2** on the producer dimension: when a producer moves state, the old row is closed
# MAGIC   (`effective_to`, `is_current = false`) and a new current row is inserted.
# MAGIC * An **audit table** with counts per batch, so you can prove `rows_in = valid + quarantined`.
# MAGIC
# MAGIC Order matters: programs → producers → payments (payments check producers for orphans).

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../src"))

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from fsa_lakehouse import config as C
from fsa_lakehouse import transforms as T
from fsa_lakehouse.streaming import run_with_schema_restart

# On serverless, foreachBatch functions run in a SEPARATE Python process (the streaming runner).
# That process doesn't know about the sys.path change above, so it can't find `fsa_lakehouse`.
# Fix: pass the folder path as a plain string, and import the project code inside the functions.
SRC_PATH = os.path.abspath("../src")
AUDIT_TABLE = C.OPS_RUN_AUDIT


def load_project():
    import sys
    if SRC_PATH not in sys.path:
        sys.path.insert(0, SRC_PATH)
    from fsa_lakehouse import config, transforms
    return config, transforms

# COMMAND ----------

# MAGIC %md ### dim_program (small reference table: full refresh is fine)

# COMMAND ----------

(spark.table(f"{C.CATALOG}.{C.SCHEMA_BRONZE}.programs_raw")
 .select(
     F.upper(F.trim("program_code")).alias("program_code"),
     F.trim("program_name").alias("program_name"),
     F.trim("program_category").alias("program_category"),
     (F.lower("subject_to_arc_plc_limit") == "true").alias("subject_to_arc_plc_limit"),
 )
 .dropDuplicates(["program_code"])
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(C.SILVER_PROGRAMS))

display(spark.table(C.SILVER_PROGRAMS))

# COMMAND ----------

# MAGIC %md ### dim_producer: SCD Type 2 via MERGE

# COMMAND ----------

def audit(sess, layer, entity, batch_id, rows_in, valid, quarantined, deduped):
    (sess.createDataFrame(
        [(layer, entity, batch_id, rows_in, valid, quarantined, deduped)],
        "layer string, entity string, batch_id long, rows_in long, rows_valid long, "
        "rows_quarantined long, rows_after_dedupe long")
     .withColumn("run_ts", F.current_timestamp())
     .write.mode("append").option("mergeSchema", "true").saveAsTable(AUDIT_TABLE))


def upsert_producers(batch_df, batch_id):
    C, T = load_project()   # runs inside the streaming runner
    sess = batch_df.sparkSession
    updates = T.standardize_producers(batch_df).filter(F.col("producer_id").isNotNull())
    current = sess.table(C.SILVER_PRODUCERS).filter("is_current = true")
    staged = T.scd2_staged_updates(updates, current)

    (DeltaTable.forName(sess, C.SILVER_PRODUCERS).alias("t")
     .merge(staged.alias("s"), "t.producer_id = s.merge_key AND t.is_current = true")
     .whenMatchedUpdate(
         condition="t.row_hash <> s.row_hash",
         set={"is_current": "false", "effective_to": "s.updated_at"})
     .whenNotMatchedInsert(values={
         "producer_id": "s.producer_id", "producer_name": "s.producer_name",
         "entity_type": "s.entity_type", "tin": "s.tin", "state_code": "s.state_code",
         "county_fips": "s.county_fips", "farm_type": "s.farm_type",
         "agi_over_900k": "s.agi_over_900k", "row_hash": "s.row_hash",
         "effective_from": "s.updated_at", "effective_to": "CAST(NULL AS TIMESTAMP)",
         "is_current": "true"})
     .execute())
    n = batch_df.count()
    audit(sess, "silver", "dim_producer", batch_id, n, n, 0, staged.count())


def start_producers():
    return (spark.readStream.table(C.BRONZE_PRODUCERS)
            .writeStream.foreachBatch(upsert_producers)
            .option("checkpointLocation", f"{C.CHECKPOINT_ROOT}/silver_producers")
            .trigger(availableNow=True).start())

run_with_schema_restart(start_producers)

# COMMAND ----------

# MAGIC %md ### payments: validate → quarantine → dedupe → orphan check → MERGE

# COMMAND ----------

SILVER_PAYMENT_COLS = [
    "payment_id", "producer_id", "program_code", "program_year", "payment_amount",
    "payment_date", "state_code", "county_fips", "payment_status", "disbursement_method",
    "source_system", "extracted_at", "is_late_arrival", "is_orphan_producer", "_ingest_file",
]


def upsert_payments(batch_df, batch_id):
    C, T = load_project()   # runs inside the streaming runner
    sess = batch_df.sparkSession
    flagged = T.add_dq_flags(T.standardize_payments(batch_df))
    valid, quarantine = T.split_valid_quarantine(flagged)

    deduped = T.dedupe_latest(valid, ["payment_id"], "extracted_at")
    deduped = T.flag_orphans(deduped, sess.table(C.SILVER_PRODUCERS))
    deduped = (deduped.select(*SILVER_PAYMENT_COLS)
               .withColumn("_silver_updated_at", F.current_timestamp()))

    (DeltaTable.forName(sess, C.SILVER_PAYMENTS).alias("t")
     .merge(deduped.alias("s"), "t.payment_id = s.payment_id")
     .whenMatchedUpdateAll(condition="s.extracted_at > t.extracted_at")   # only a newer extract wins
     .whenNotMatchedInsertAll()
     .execute())

    (quarantine
     .select("payment_id", "producer_id", "program_code", "state_code", "payment_status",
             F.col("payment_amount").cast("string").alias("payment_amount"),
             F.col("payment_date").cast("string").alias("payment_date"),
             "dq_errors", "_rescued_data", "_ingest_file")
     .withColumn("_quarantined_at", F.current_timestamp())
     .withColumn("_batch_id", F.lit(batch_id))
     .write.mode("append").option("mergeSchema", "true").saveAsTable(C.SILVER_PAYMENTS_QUARANTINE))

    audit(sess, "silver", "payments", batch_id,
          flagged.count(), valid.count(), quarantine.count(), deduped.count())


def start_payments():
    return (spark.readStream.table(C.BRONZE_PAYMENTS)
            .writeStream.foreachBatch(upsert_payments)
            .option("checkpointLocation", f"{C.CHECKPOINT_ROOT}/silver_payments")
            .trigger(availableNow=True).start())

run_with_schema_restart(start_payments)

# COMMAND ----------

# MAGIC %md ### Check your work

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {C.OPS_RUN_AUDIT} ORDER BY run_ts DESC"))

# COMMAND ----------

# Why was each row quarantined?
display(spark.sql(f"""
SELECT err, count(*) AS rows
FROM {C.SILVER_PAYMENTS_QUARANTINE} LATERAL VIEW explode(dq_errors) e AS err
GROUP BY err ORDER BY rows DESC
"""))

# COMMAND ----------

# SCD2 in action: producers that have more than one version
display(spark.sql(f"""
SELECT producer_id, producer_sk, state_code, farm_type, agi_over_900k,
       effective_from, effective_to, is_current
FROM {C.SILVER_PRODUCERS}
WHERE producer_id IN (SELECT producer_id FROM {C.SILVER_PRODUCERS} GROUP BY 1 HAVING count(*) > 1)
ORDER BY producer_id, effective_from
"""))
