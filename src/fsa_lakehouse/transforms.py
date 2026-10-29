"""Pure PySpark transformations, with no Databricks-only APIs.

Keeping the business logic here (instead of inside notebooks) is what makes it
unit-testable on a laptop with plain PySpark (see tests/). The notebooks only
handle I/O: Auto Loader, MERGE, writing tables.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import config

PRODUCER_TRACKED_COLS = [
    "producer_name", "entity_type", "tin", "state_code", "county_fips",
    "farm_type", "agi_over_900k",
]


# ---------------------------------------------------------------- payments ---

def standardize_payments(df: DataFrame) -> DataFrame:
    """Bronze -> typed, trimmed, upper-cased. No rows dropped here."""
    out = (
        df
        .withColumn("payment_id", F.trim("payment_id"))
        .withColumn("producer_id", F.trim("producer_id"))
        .withColumn("program_code", F.upper(F.trim("program_code")))
        .withColumn("state_code", F.upper(F.trim("state_code")))
        .withColumn("payment_status", F.upper(F.trim("payment_status")))
        .withColumn("program_year", F.col("program_year").cast("int"))
        .withColumn("payment_amount", F.col("payment_amount").cast("decimal(14,2)"))
        .withColumn("payment_date", F.to_date("payment_date"))
        .withColumn("extracted_at", F.to_timestamp("extracted_at"))
    )
    if "disbursement_method" not in out.columns:   # schema drift: column only exists from day 4
        out = out.withColumn("disbursement_method", F.lit(None).cast("string"))
    return out


def add_dq_flags(df: DataFrame, batch_date_col: str = "extracted_at") -> DataFrame:
    """Attach `dq_errors`: an array of rule names this row violates (empty = clean)."""
    valid_programs = F.array(*[F.lit(p) for p in sorted(config.VALID_PROGRAMS)])
    valid_states = F.array(*[F.lit(s) for s in sorted(config.VALID_STATES)])
    valid_status = F.array(*[F.lit(s) for s in sorted(config.VALID_STATUSES)])

    rules = [
        ("missing_payment_id", F.col("payment_id").isNull() | (F.col("payment_id") == "")),
        ("missing_producer_id", F.col("producer_id").isNull() | (F.col("producer_id") == "")),
        ("invalid_program_code", ~F.array_contains(valid_programs, F.coalesce(F.col("program_code"), F.lit("")))),
        ("invalid_state_code", ~F.array_contains(valid_states, F.coalesce(F.col("state_code"), F.lit("")))),
        ("invalid_status", ~F.array_contains(valid_status, F.coalesce(F.col("payment_status"), F.lit("")))),
        ("non_positive_amount", F.col("payment_amount").isNull() | (F.col("payment_amount") <= 0)),
        ("missing_payment_date", F.col("payment_date").isNull()),
    ]
    flags = [F.when(cond, F.lit(name)) for name, cond in rules]
    out = df.withColumn("dq_errors", F.filter(F.array(*flags), lambda x: x.isNotNull()))
    # Late arrival is not an error, just a tag. Good talking point: "late != bad".
    return out.withColumn(
        "is_late_arrival",
        F.datediff(F.to_date(F.col(batch_date_col)), F.col("payment_date")) > 14,
    )


def split_valid_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = df.filter(F.size("dq_errors") == 0)
    quarantine = df.filter(F.size("dq_errors") > 0)
    return valid, quarantine


def dedupe_latest(df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
    """Keep one row per key: the one with the greatest order_col (latest extract wins)."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def flag_orphans(payments: DataFrame, producers: DataFrame) -> DataFrame:
    """Left-anti check: payments whose producer_id does not exist in the producer dim."""
    known = producers.select("producer_id").distinct().withColumn("_known", F.lit(True))
    return (
        payments.join(known, "producer_id", "left")
        .withColumn("is_orphan_producer", F.col("_known").isNull())
        .drop("_known")
    )


# --------------------------------------------------------------- producers ---

def standardize_producers(df: DataFrame) -> DataFrame:
    out = df
    for c in PRODUCER_TRACKED_COLS + ["producer_id"]:
        out = out.withColumn(c, F.trim(F.col(c)))
    return (
        out
        .withColumn("state_code", F.upper("state_code"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("~")) for c in PRODUCER_TRACKED_COLS]), 256))
    )


def scd2_staged_updates(updates: DataFrame, current_dim: DataFrame) -> DataFrame:
    """Build the MERGE source for an SCD Type 2 dimension (the classic 'merge_key' trick).

    updates:      standardized producer changes (one batch, may contain repeats)
    current_dim:  rows of the dimension where is_current = true

    Output rows:
      merge_key = producer_id  -> matches the current row; MERGE expires it
                                 (or inserts if the producer is brand new)
      merge_key = NULL         -> never matches; MERGE inserts it as the new current version
    Rows whose attributes did not actually change are dropped (idempotent reruns).
    """
    latest = dedupe_latest(updates, ["producer_id"], "updated_at")
    cur = current_dim.select("producer_id", F.col("row_hash").alias("_cur_hash"))
    joined = latest.join(cur, "producer_id", "left")

    new_producers = joined.filter(F.col("_cur_hash").isNull())
    changed = joined.filter(F.col("_cur_hash").isNotNull() & (F.col("_cur_hash") != F.col("row_hash")))

    cols = [c for c in latest.columns]
    expire_or_insert_new = (
        new_producers.unionByName(changed)
        .select(F.col("producer_id").alias("merge_key"), *cols)
    )
    insert_new_version = changed.select(F.lit(None).cast("string").alias("merge_key"), *cols)
    return expire_or_insert_new.unionByName(insert_new_version)


# -------------------------------------------------------------------- gold ---

def payments_by_state_program_year(payments: DataFrame, programs: DataFrame) -> DataFrame:
    issued = payments.filter(F.col("payment_status") == "ISSUED")
    return (
        issued.join(programs.select("program_code", "program_name", "program_category"), "program_code", "left")
        .groupBy("state_code", "program_year", "program_code", "program_name", "program_category")
        .agg(
            F.count("*").alias("payment_count"),
            F.countDistinct("producer_id").alias("producer_count"),
            F.sum("payment_amount").alias("total_paid"),
            F.round(F.avg("payment_amount"), 2).alias("avg_payment"),
            F.max("payment_date").alias("latest_payment_date"),
        )
    )


def producer_payment_limit_check(payments: DataFrame, producers_current: DataFrame,
                                 programs: DataFrame,
                                 limit: float = config.ARC_PLC_PAYMENT_LIMIT) -> DataFrame:
    """Per producer and program year: ARC/PLC total vs the payment limitation."""
    limited = programs.filter(F.col("subject_to_arc_plc_limit")).select("program_code")
    net = (
        payments.filter(F.col("payment_status").isin("ISSUED", "REVERSED"))
        .withColumn("signed_amount",
                    F.when(F.col("payment_status") == "REVERSED", -F.col("payment_amount"))
                     .otherwise(F.col("payment_amount")))
        .join(limited, "program_code", "inner")
        .groupBy("producer_id", "program_year")
        .agg(F.sum("signed_amount").alias("arc_plc_net_paid"))
    )
    return (
        net.join(producers_current.select("producer_id", "producer_name", "state_code",
                                          "entity_type", "agi_over_900k"), "producer_id", "left")
        .withColumn("payment_limit", F.lit(limit).cast("decimal(14,2)"))
        .withColumn("over_limit", F.col("arc_plc_net_paid") > F.col("payment_limit"))
        .withColumn("agi_ineligible", F.col("agi_over_900k") == "Y")
        .withColumn("needs_review", F.col("over_limit") | F.col("agi_ineligible"))
    )
