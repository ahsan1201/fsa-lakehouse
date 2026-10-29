"""Synthetic USDA Farm Service Agency (FSA) source-system extracts.

Simulates what a legacy FSA system would drop into a landing bucket each day:

  programs/   programs.csv                 one-time reference data
  producers/  producers_<date>.csv         full snapshot on day 1, then daily changes (CDC)
  payments/   <date>/payments_<n>.json     newline-delimited JSON, daily batches

Real-world mess is injected on purpose, because that is what the pipeline
has to survive. Every defect is controlled by a flag so you can switch it on
in the "break it" exercises:

  * duplicate payment records (same payment_id re-sent)
  * negative amounts, unknown program codes, invalid state codes
  * missing producer_id, and "orphan" producer_ids that never existed
  * late-arriving records (payment_date weeks in the past)
  * schema drift: a new column `disbursement_method` appears from DRIFT_DAY on
  * one corrupt JSON line (truncated record)

Only the standard library is used, so this runs both in a Databricks
notebook (writing to /Volumes/...) and on a laptop (writing to a temp folder).
"""

from __future__ import annotations

import csv
import json
import os
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta

PROGRAMS = [
    # code, name, category, subject to ARC/PLC limit
    ("ARC-CO", "Agriculture Risk Coverage - County", "Commodity", True),
    ("PLC", "Price Loss Coverage", "Commodity", True),
    ("ELAP", "Emergency Assistance for Livestock, Honeybees and Farm-raised Fish", "Disaster", False),
    ("LFP", "Livestock Forage Disaster Program", "Disaster", False),
    ("CRP", "Conservation Reserve Program", "Conservation", False),
    ("ECAP", "Emergency Commodity Assistance Program", "Disaster", False),
]

# (state, county FIPS samples)
STATES = {
    "IA": ["19153", "19113", "19163"],
    "KS": ["20173", "20091", "20045"],
    "NE": ["31055", "31109", "31079"],
    "TX": ["48201", "48113", "48303"],
    "VA": ["51059", "51107", "51003"],
    "MN": ["27053", "27123", "27109"],
    "CA": ["06019", "06077", "06107"],
    "ND": ["38017", "38015", "38035"],
}

FARM_TYPES = ["Row Crop", "Livestock", "Dairy", "Specialty Crop", "Mixed"]
ENTITY_TYPES = ["Individual", "LLC", "Partnership", "Corporation", "Trust"]
FIRST = ["James", "Mary", "Robert", "Linda", "Ahmed", "Maria", "Wei", "Sarah", "Juan", "Emily"]
LAST = ["Miller", "Johnson", "Schmidt", "Nguyen", "Garcia", "Olson", "Patel", "Brown", "Larson", "Khan"]
FARM_SUFFIX = ["Farms", "Acres", "Ranch", "Ag LLC", "Family Farm"]
DISBURSEMENT = ["EFT", "CHECK", "TREASURY_OFFSET"]


@dataclass
class DefectRates:
    duplicate: float = 0.02
    negative_amount: float = 0.005
    bad_program: float = 0.004
    bad_state: float = 0.004
    null_producer: float = 0.004
    orphan_producer: float = 0.003
    late_arrival: float = 0.03
    corrupt_line: bool = True   # one truncated JSON line per batch on CORRUPT_DAY


DRIFT_DAY = 4     # schema drift starts on batch day 4
CORRUPT_DAY = 3   # a corrupt JSON line appears on batch day 3


def _producer_id(i: int) -> str:
    return f"P{100000 + i}"


def _fake_tin(rng: random.Random) -> str:
    # Clearly fake 9-digit taxpayer id, formatted like an EIN (9X-XXXXXXX = invalid prefix)
    return f"9{rng.randint(0, 9)}-{rng.randint(1000000, 9999999)}"


def _make_producer(i: int, rng: random.Random, as_of: datetime) -> dict:
    state = rng.choice(list(STATES))
    entity = rng.choice(ENTITY_TYPES)
    if entity == "Individual":
        name = f"{rng.choice(FIRST)} {rng.choice(LAST)}"
    else:
        name = f"{rng.choice(LAST)} {rng.choice(FARM_SUFFIX)}"
    return {
        "producer_id": _producer_id(i),
        "producer_name": name,
        "entity_type": entity,
        "tin": _fake_tin(rng),
        "state_code": state,
        "county_fips": rng.choice(STATES[state]),
        "farm_type": rng.choice(FARM_TYPES),
        "agi_over_900k": "Y" if rng.random() < 0.03 else "N",
        "updated_at": as_of.isoformat(timespec="seconds"),
        "op": "I",
    }


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_programs(root: str) -> str:
    out_dir = os.path.join(root, "programs")
    _ensure_dir(out_dir)
    path = os.path.join(out_dir, "programs.csv")
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["program_code", "program_name", "program_category", "subject_to_arc_plc_limit"])
        for row in PROGRAMS:
            w.writerow(row)
    return path


def write_producers(root: str, batch_day: int, batch_date: date, n_producers: int, seed: int) -> str:
    """Day 1: full snapshot. Later days: ~3% of producers change (CDC records)."""
    out_dir = os.path.join(root, "producers")
    _ensure_dir(out_dir)
    as_of = datetime.combine(batch_date, datetime.min.time()) + timedelta(hours=2)
    base_rng = random.Random(seed)
    base = [_make_producer(i, base_rng, as_of) for i in range(n_producers)]

    if batch_day == 1:
        rows = base
    else:
        rng = random.Random(seed * 1000 + batch_day)
        changed = rng.sample(base, k=max(1, int(n_producers * 0.03)))
        rows = []
        for p in changed:
            p = dict(p)
            change = rng.choice(["move", "farm_type", "agi"])
            if change == "move":
                new_state = rng.choice([s for s in STATES if s != p["state_code"]])
                p["state_code"] = new_state
                p["county_fips"] = rng.choice(STATES[new_state])
            elif change == "farm_type":
                p["farm_type"] = rng.choice([t for t in FARM_TYPES if t != p["farm_type"]])
            else:
                p["agi_over_900k"] = "N" if p["agi_over_900k"] == "Y" else "Y"
            p["updated_at"] = (as_of + timedelta(minutes=rng.randint(0, 600))).isoformat(timespec="seconds")
            p["op"] = "U"
            rows.append(p)
        # a handful of brand-new producers registering
        for j in range(3):
            rows.append(_make_producer(n_producers + batch_day * 10 + j, rng, as_of))

    path = os.path.join(out_dir, f"producers_{batch_date.isoformat()}.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    return path


def _payment(rng: random.Random, seq: int, batch_day: int, batch_date: date,
             n_producers: int, rates: DefectRates) -> dict:
    program_code, _, _, limited = rng.choice(PROGRAMS)
    pid = _producer_id(rng.randrange(n_producers))
    state = rng.choice(list(STATES))
    if limited:
        amount = round(rng.lognormvariate(9.3, 0.9), 2)   # ARC/PLC: median ~ $11k, long tail
    else:
        amount = round(rng.lognormvariate(8.5, 1.0), 2)
    pay_date = batch_date - timedelta(days=rng.randint(0, 1))
    rec = {
        "payment_id": f"FSA{batch_date:%Y%m%d}{seq:07d}",
        "producer_id": pid,
        "program_code": program_code,
        "program_year": batch_date.year - (1 if program_code in ("ARC-CO", "PLC") else 0),
        "payment_amount": amount,
        "payment_date": pay_date.isoformat(),
        "state_code": state,
        "county_fips": rng.choice(STATES[state]),
        "payment_status": rng.choices(["ISSUED", "PENDING", "REVERSED"], weights=[90, 8, 2])[0],
        "source_system": "FSA-NPS",
        "extracted_at": (datetime.combine(batch_date, datetime.min.time())
                         + timedelta(hours=1, seconds=seq % 3600)).isoformat(timespec="seconds"),
    }
    # --- defects ---
    r = rng.random
    if r() < rates.negative_amount:
        rec["payment_amount"] = -abs(rec["payment_amount"])
    if r() < rates.bad_program:
        rec["program_code"] = rng.choice(["XYZ", "arc-co ", ""])   # unknown / dirty codes
    if r() < rates.bad_state:
        rec["state_code"] = rng.choice(["ZZ", "Iowa", None])
    if r() < rates.null_producer:
        rec["producer_id"] = None
    elif r() < rates.orphan_producer:
        rec["producer_id"] = f"P9{rng.randint(10000, 99999)}"  # never registered
    if r() < rates.late_arrival:
        rec["payment_date"] = (batch_date - timedelta(days=rng.randint(20, 90))).isoformat()
    if batch_day >= DRIFT_DAY:
        rec["disbursement_method"] = rng.choice(DISBURSEMENT)
    return rec


def write_payments(root: str, batch_day: int, batch_date: date, n_payments: int,
                   n_producers: int, seed: int, rates: DefectRates | None = None,
                   files_per_batch: int = 2) -> list[str]:
    rates = rates or DefectRates()
    rng = random.Random(seed * 7919 + batch_day)
    out_dir = os.path.join(root, "payments", batch_date.isoformat())
    _ensure_dir(out_dir)

    records = [_payment(rng, i, batch_day, batch_date, n_producers, rates) for i in range(n_payments)]
    # Duplicates: some records re-sent, a few of them with a corrected amount
    # (the "latest extract wins" rule in silver must handle this)
    dupes = []
    for rec in rng.sample(records, k=int(n_payments * rates.duplicate)):
        d = dict(rec)
        if rng.random() < 0.3 and isinstance(d["payment_amount"], float):
            d["payment_amount"] = round(d["payment_amount"] * 1.05, 2)
        d["extracted_at"] = (datetime.fromisoformat(d["extracted_at"]) + timedelta(hours=3)).isoformat(timespec="seconds")
        dupes.append(d)
    records.extend(dupes)
    rng.shuffle(records)

    paths = []
    chunk = -(-len(records) // files_per_batch)
    for n in range(files_per_batch):
        part = records[n * chunk:(n + 1) * chunk]
        path = os.path.join(out_dir, f"payments_{batch_date:%Y%m%d}_{n + 1:03d}.json")
        with open(path, "w") as f:
            for i, rec in enumerate(part):
                line = json.dumps(rec)
                if rates.corrupt_line and batch_day == CORRUPT_DAY and n == 0 and i == 5:
                    line = line[: len(line) // 2]   # truncated record
                f.write(line + "\n")
        paths.append(path)
    return paths


def generate_batch(root: str, batch_day: int, start_date: date = date(2026, 10, 1),
                   n_payments: int = 5000, n_producers: int = 2000, seed: int = 42,
                   rates: DefectRates | None = None) -> dict:
    """Generate one day's drop. batch_day starts at 1."""
    if batch_day < 1:
        raise ValueError("batch_day starts at 1")
    batch_date = start_date + timedelta(days=batch_day - 1)
    out = {"batch_date": batch_date.isoformat()}
    if batch_day == 1:
        out["programs"] = write_programs(root)
    out["producers"] = write_producers(root, batch_day, batch_date, n_producers, seed)
    out["payments"] = write_payments(root, batch_day, batch_date, n_payments, n_producers, seed, rates)
    return out


if __name__ == "__main__":
    import argparse
    import tempfile

    ap = argparse.ArgumentParser(description="Generate synthetic FSA extracts")
    ap.add_argument("--root", default=os.path.join(tempfile.gettempdir(), "fsa_landing"))
    ap.add_argument("--days", type=int, default=5)
    args = ap.parse_args()
    for d in range(1, args.days + 1):
        print(generate_batch(args.root, d))
