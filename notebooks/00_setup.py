# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup: catalog, schemas, volumes, tables
# MAGIC
# MAGIC Run once. Re-running is safe (everything is `IF NOT EXISTS`).
# MAGIC
# MAGIC **What you're learning:** Unity Catalog's `catalog.schema.table` namespace, Volumes as governed
# MAGIC file storage (your stand-in for S3), and declaring Delta tables up front with identity columns
# MAGIC and liquid clustering.
# MAGIC
# MAGIC > If `CREATE CATALOG` fails with a permissions error on Free Edition, open
# MAGIC > `src/fsa_lakehouse/config.py`, set `CATALOG = "workspace"`, and rerun this notebook.

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../src"))

from fsa_lakehouse import config as C

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {C.CATALOG}")
for schema, comment in [
    (C.SCHEMA_LANDING, "Raw file drops from FSA source systems (stand-in for an S3 landing bucket)"),
    (C.SCHEMA_BRONZE, "Raw, append-only copies of source data plus ingestion metadata"),
    (C.SCHEMA_SILVER, "Cleaned, conformed, deduplicated entities"),
    (C.SCHEMA_GOLD, "Business-level aggregates for BI and analytics"),
    (C.SCHEMA_OPS, "Checkpoints, inferred schemas, audit tables"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C.CATALOG}.{schema} COMMENT '{comment}'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {C.CATALOG}.{C.SCHEMA_LANDING}.{C.VOLUME_RAW}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {C.CATALOG}.{C.SCHEMA_OPS}.{C.VOLUME_CHECKPOINTS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver tables, declared explicitly
# MAGIC Bronze tables are created by Auto Loader from the data. Silver tables get a contract:
# MAGIC typed columns, a surrogate key, and clustering keys chosen from how they'll be queried.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {C.SILVER_PRODUCERS} (
  producer_sk     BIGINT GENERATED ALWAYS AS IDENTITY,
  producer_id     STRING NOT NULL,
  producer_name   STRING,
  entity_type     STRING,
  tin             STRING COMMENT 'Taxpayer ID (PII, masked by policy in 05_governance)',
  state_code      STRING,
  county_fips     STRING,
  farm_type       STRING,
  agi_over_900k   STRING,
  row_hash        STRING,
  effective_from  TIMESTAMP,
  effective_to    TIMESTAMP,
  is_current      BOOLEAN
)
CLUSTER BY (producer_id)
COMMENT 'Producer dimension, SCD Type 2: one row per version of a producer'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {C.SILVER_PAYMENTS} (
  payment_id          STRING NOT NULL,
  producer_id         STRING,
  program_code        STRING,
  program_year        INT,
  payment_amount      DECIMAL(14,2),
  payment_date        DATE,
  state_code          STRING,
  county_fips         STRING,
  payment_status      STRING,
  disbursement_method STRING,
  source_system       STRING,
  extracted_at        TIMESTAMP,
  is_late_arrival     BOOLEAN,
  is_orphan_producer  BOOLEAN,
  _ingest_file        STRING,
  _silver_updated_at  TIMESTAMP
)
CLUSTER BY (program_year, state_code)
COMMENT 'One row per FSA payment, deduplicated (latest extract wins)'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {C.OPS_RUN_AUDIT} (
  run_ts        TIMESTAMP,
  layer         STRING,
  entity        STRING,
  batch_id      BIGINT,
  rows_in       BIGINT,
  rows_valid    BIGINT,
  rows_quarantined BIGINT,
  rows_after_dedupe BIGINT
)
COMMENT 'Row counts per micro-batch: lets you answer "did every record land somewhere?"'
""")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {C.CATALOG}.{C.SCHEMA_SILVER}"))
print("Landing root:", C.LANDING_ROOT)
