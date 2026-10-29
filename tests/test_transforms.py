"""Unit tests for the business logic. Run locally: pytest -q"""

from datetime import date

from pyspark.sql import Row
from pyspark.sql import functions as F

from fsa_lakehouse import transforms as T
from fsa_lakehouse.generator import DRIFT_DAY, generate_batch


def _payment(**kw):
    base = dict(payment_id="FSA1", producer_id="P100001", program_code="PLC", program_year=2025,
                payment_amount=1000.0, payment_date="2026-10-01", state_code="IA",
                county_fips="19153", payment_status="ISSUED", source_system="FSA-NPS",
                extracted_at="2026-10-01T01:00:00")
    base.update(kw)
    return Row(**base)


def test_standardize_trims_and_adds_drift_column(spark):
    df = spark.createDataFrame([_payment(program_code=" arc-co ", state_code="ia")])
    out = T.standardize_payments(df).first()
    assert out.program_code == "ARC-CO"
    assert out.state_code == "IA"
    assert out.disbursement_method is None
    assert str(out.payment_amount) == "1000.00"


def test_dq_rules_catch_each_defect(spark):
    rows = [
        _payment(payment_id="ok"),
        _payment(payment_id="neg", payment_amount=-5.0),
        _payment(payment_id="prog", program_code="XYZ"),
        _payment(payment_id="state", state_code="ZZ"),
        _payment(payment_id="noprod", producer_id=None),
    ]
    df = T.add_dq_flags(T.standardize_payments(spark.createDataFrame(rows)))
    errs = {r.payment_id: set(r.dq_errors) for r in df.collect()}
    assert errs["ok"] == set()
    assert errs["neg"] == {"non_positive_amount"}
    assert errs["prog"] == {"invalid_program_code"}
    assert errs["state"] == {"invalid_state_code"}
    assert errs["noprod"] == {"missing_producer_id"}

    valid, quarantine = T.split_valid_quarantine(df)
    assert valid.count() == 1 and quarantine.count() == 4


def test_late_arrival_is_tagged_not_rejected(spark):
    df = T.add_dq_flags(T.standardize_payments(
        spark.createDataFrame([_payment(payment_date="2026-08-01")])))
    r = df.first()
    assert r.is_late_arrival is True
    assert r.dq_errors == []


def test_dedupe_latest_extract_wins(spark):
    df = spark.createDataFrame([
        _payment(payment_amount=100.0, extracted_at="2026-10-01T01:00:00"),
        _payment(payment_amount=105.0, extracted_at="2026-10-01T04:00:00"),
    ])
    out = T.dedupe_latest(T.standardize_payments(df), ["payment_id"], "extracted_at").collect()
    assert len(out) == 1 and float(out[0].payment_amount) == 105.0


def test_orphans_flagged(spark):
    pays = spark.createDataFrame([_payment(producer_id="P1"), _payment(payment_id="2", producer_id="P_GHOST")])
    prods = spark.createDataFrame([Row(producer_id="P1")])
    flags = {r.producer_id: r.is_orphan_producer for r in T.flag_orphans(pays, prods).collect()}
    assert flags == {"P1": False, "P_GHOST": True}


def _producer(pid, state="IA", farm="Row Crop", ts="2026-10-01T02:00:00"):
    return Row(producer_id=pid, producer_name="Olson Farms", entity_type="LLC", tin="90-1234567",
               state_code=state, county_fips="19153", farm_type=farm, agi_over_900k="N",
               updated_at=ts, op="U")


def test_scd2_staging(spark):
    current = T.standardize_producers(spark.createDataFrame([
        _producer("P1"), _producer("P2"),
    ]))
    updates = T.standardize_producers(spark.createDataFrame([
        _producer("P1", state="KS", ts="2026-10-02T03:00:00"),   # real change
        _producer("P1", state="NE", ts="2026-10-02T01:00:00"),   # older change in same batch: ignored
        _producer("P2"),                                          # no change: dropped
        _producer("P3", ts="2026-10-02T03:00:00"),               # brand new
    ]))
    staged = T.scd2_staged_updates(updates, current).collect()
    by_key = sorted([(r.merge_key, r.producer_id, r.state_code) for r in staged], key=str)
    assert by_key == sorted([
        ("P1", "P1", "KS"),    # expires current P1
        (None, "P1", "KS"),    # inserts new P1 version
        ("P3", "P3", "IA"),    # inserts new producer
    ], key=str)


def test_gold_limit_check(spark):
    programs = spark.createDataFrame([
        Row(program_code="PLC", program_name="Price Loss Coverage", program_category="Commodity", subject_to_arc_plc_limit=True),
        Row(program_code="CRP", program_name="Conservation", program_category="Conservation", subject_to_arc_plc_limit=False),
    ])
    pays = T.standardize_payments(spark.createDataFrame([
        _payment(payment_id="a", payment_amount=100000.0),
        _payment(payment_id="b", payment_amount=40000.0),
        _payment(payment_id="c", payment_amount=500000.0, program_code="CRP"),  # not limited
        _payment(payment_id="d", payment_amount=10000.0, payment_status="REVERSED"),
    ]))
    prods = T.standardize_producers(spark.createDataFrame([_producer("P100001")]))
    r = T.producer_payment_limit_check(pays, prods, programs).first()
    assert float(r.arc_plc_net_paid) == 130000.0
    assert r.over_limit is True and r.needs_review is True

    summary = T.payments_by_state_program_year(pays, programs)
    assert summary.filter(F.col("program_code") == "PLC").first().payment_count == 2


def test_generator_end_to_end_through_transforms(spark, tmp_path):
    root = str(tmp_path)
    for d in range(1, DRIFT_DAY + 1):
        generate_batch(root, d, start_date=date(2026, 10, 1), n_payments=400, n_producers=100)
    # permissive read mimics Auto Loader's rescue behaviour for the corrupt line
    raw = (spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt")
           .json(f"{root}/payments/*/*.json"))
    assert "disbursement_method" in raw.columns           # drift showed up
    raw.cache()
    assert raw.filter("_corrupt IS NOT NULL").count() == 1  # the truncated line
    clean = raw.filter("_corrupt IS NULL").drop("_corrupt")
    flagged = T.add_dq_flags(T.standardize_payments(clean))
    valid, quarantine = T.split_valid_quarantine(flagged)
    deduped = T.dedupe_latest(valid, ["payment_id"], "extracted_at")
    assert quarantine.count() > 0
    assert deduped.count() < valid.count()                   # duplicates removed
    assert deduped.select("payment_id").distinct().count() == deduped.count()
