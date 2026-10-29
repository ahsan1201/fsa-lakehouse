"""Phase 5 · The same pipeline, rebuilt as a Lakeflow Spark Declarative Pipeline.

Notebooks 02-04 are the "imperative" version: you manage checkpoints, MERGE, quarantine,
and run order yourself. Here you declare WHAT each table is, and Databricks works out
the order, checkpoints, retries and the SCD2 logic.

Setup (Jobs & Pipelines → Create → ETL pipeline):
  * Source code: this file
  * Default catalog: fsa     Default schema: sdp   (create the schema first: CREATE SCHEMA fsa.sdp)
  * Configuration:  landing_root = /Volumes/fsa/landing/raw
  * Serverless: on
Run it, then open the pipeline graph and the data-quality tab for each table.

Interview point: compare this file (about 100 lines) with notebooks 02-04. Know the trade-off:
declarative = less code and built-in expectations and lineage; imperative = full control (custom MERGE
conditions, audit tables, unusual sources).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing_root", "/Volumes/fsa/landing/raw")  # noqa: F821 (spark is provided)

VALID_PROGRAMS = "('ARC-CO','PLC','ELAP','LFP','CRP','ECAP')"


# ------------------------------------------------------------------ bronze --

@dp.table(comment="Raw FSA payments, all strings, via Auto Loader")
def payments_bronze():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(f"{LANDING}/payments")
        .withColumn("_ingest_file", F.col("_metadata.file_path"))
    )


@dp.table(comment="Raw producer snapshot + CDC files")
def producers_bronze():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(f"{LANDING}/producers")
    )


@dp.materialized_view(comment="Program reference data")
def dim_program():
    return (
        spark.read.option("header", "true").csv(f"{LANDING}/programs")  # noqa: F821
        .withColumn("subject_to_arc_plc_limit", F.lower("subject_to_arc_plc_limit") == "true")
    )


# ------------------------------------------------------------------ silver --
# Expectations replace the hand-written quarantine logic.
#   expect          -> keep the row, record the violation in metrics
#   expect_or_drop  -> drop the row, record it
#   expect_or_fail  -> stop the update (use for "this should never happen")

@dp.table(comment="Typed and validated payments (may still contain duplicates)")
@dp.expect_or_drop("has_payment_id", "payment_id IS NOT NULL")
@dp.expect_or_drop("has_producer_id", "producer_id IS NOT NULL")
@dp.expect_or_drop("valid_program", f"program_code IN {VALID_PROGRAMS}")
@dp.expect_or_drop("positive_amount", "payment_amount > 0")
@dp.expect("not_late_arrival", "datediff(to_date(extracted_at), payment_date) <= 14")
def payments_validated():
    return (
        spark.readStream.table("payments_bronze")  # noqa: F821
        .withColumn("program_code", F.upper(F.trim("program_code")))
        .withColumn("state_code", F.upper(F.trim("state_code")))
        .withColumn("program_year", F.col("program_year").cast("int"))
        .withColumn("payment_amount", F.col("payment_amount").cast("decimal(14,2)"))
        .withColumn("payment_date", F.to_date("payment_date"))
        .withColumn("extracted_at", F.to_timestamp("extracted_at"))
    )


# Dedupe + upsert with AUTO CDC: "latest extract wins" on payment_id, declared instead of coded.
dp.create_streaming_table("payments", comment="One row per payment (SCD Type 1)")
dp.create_auto_cdc_flow(
    target="payments",
    source="payments_validated",
    keys=["payment_id"],
    sequence_by=F.col("extracted_at"),
    stored_as_scd_type=1,
)

# SCD Type 2 producer dimension: compare these ~10 lines to upsert_producers() in 03_silver.
dp.create_streaming_table("dim_producer", comment="Producer history, SCD Type 2")
dp.create_auto_cdc_flow(
    target="dim_producer",
    source="producers_bronze",
    keys=["producer_id"],
    sequence_by=F.to_timestamp("updated_at"),
    except_column_list=["op", "_rescued_data"],
    stored_as_scd_type=2,
)


# -------------------------------------------------------------------- gold --

@dp.materialized_view(comment="Issued payments by state, program and year")
def payments_by_state_program_year():
    p = spark.read.table("payments").filter("payment_status = 'ISSUED'")  # noqa: F821
    d = spark.read.table("dim_program")  # noqa: F821
    return (
        p.join(d, "program_code", "left")
        .groupBy("state_code", "program_year", "program_code", "program_name", "program_category")
        .agg(F.count("*").alias("payment_count"),
             F.countDistinct("producer_id").alias("producer_count"),
             F.sum("payment_amount").alias("total_paid"))
    )
