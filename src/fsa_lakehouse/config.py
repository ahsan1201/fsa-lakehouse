"""Single place for names and paths, so notebooks, jobs and tests agree.

Unity Catalog's three-level namespace: catalog.schema.table.
The landing zone is a UC Volume, standing in for an S3 bucket.
On a paid AWS workspace you would point LANDING_ROOT at an
external location such as s3://usda-fsa-landing/ instead.
"""

CATALOG = "fsa"

SCHEMA_LANDING = "landing"   # holds the raw-files Volume
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
SCHEMA_OPS = "ops"           # checkpoints, schemas, audit tables

VOLUME_RAW = "raw"
VOLUME_CHECKPOINTS = "checkpoints"

LANDING_ROOT = f"/Volumes/{CATALOG}/{SCHEMA_LANDING}/{VOLUME_RAW}"
CHECKPOINT_ROOT = f"/Volumes/{CATALOG}/{SCHEMA_OPS}/{VOLUME_CHECKPOINTS}"

PAYMENTS_LANDING = f"{LANDING_ROOT}/payments"
PRODUCERS_LANDING = f"{LANDING_ROOT}/producers"
PROGRAMS_LANDING = f"{LANDING_ROOT}/programs"


def table(schema: str, name: str) -> str:
    return f"{CATALOG}.{schema}.{name}"


BRONZE_PAYMENTS = table(SCHEMA_BRONZE, "payments_raw")
BRONZE_PRODUCERS = table(SCHEMA_BRONZE, "producers_raw")
SILVER_PAYMENTS = table(SCHEMA_SILVER, "payments")
SILVER_PAYMENTS_QUARANTINE = table(SCHEMA_SILVER, "payments_quarantine")
SILVER_PRODUCERS = table(SCHEMA_SILVER, "dim_producer")  # SCD Type 2
SILVER_PROGRAMS = table(SCHEMA_SILVER, "dim_program")
GOLD_STATE_PROGRAM = table(SCHEMA_GOLD, "payments_by_state_program_year")
GOLD_PRODUCER_LIMITS = table(SCHEMA_GOLD, "producer_payment_limit_check")
OPS_RUN_AUDIT = table(SCHEMA_OPS, "pipeline_run_audit")

# Business rules (simplified from real FSA rules, good enough to talk about)
VALID_PROGRAMS = {"ARC-CO", "PLC", "ELAP", "LFP", "CRP", "ECAP"}
VALID_STATUSES = {"ISSUED", "PENDING", "REVERSED"}
# ARC/PLC combined payment limitation per person per program year
ARC_PLC_PAYMENT_LIMIT = 125_000.00

VALID_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
    "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
    "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "PR",
}
