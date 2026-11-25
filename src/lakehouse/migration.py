"""Backfill from a legacy system export, with a reconciliation report a finance or quality
team can sign off on: row counts, totals and a checksum per month, plus the exact keys
that differ."""

from __future__ import annotations

import csv
import hashlib
from collections import defaultdict
from pathlib import Path

import duckdb


def export_legacy_csv(rows: list[dict], path: Path) -> None:
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["claim_id", "claim_date", "amount", "status"])
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in w.fieldnames})


def _summ(rows) -> dict[str, dict]:
    out: dict[str, dict] = defaultdict(lambda: {"rows": 0, "amount": 0.0, "xor": 0})
    for cid, d, amt in rows:
        m = str(d)[:7]
        out[m]["rows"] += 1
        out[m]["amount"] += round(float(amt), 2)
        out[m]["xor"] ^= int(hashlib.md5(f"{cid}|{float(amt):.2f}".encode()).hexdigest()[:12], 16)
    return out


def reconcile(legacy_csv: Path, db_path: Path) -> dict:
    with legacy_csv.open() as f:
        legacy = [(r["claim_id"], r["claim_date"], r["amount"]) for r in csv.DictReader(f)]
    con = duckdb.connect(str(db_path), read_only=True)
    lake = con.execute("SELECT claim_id, claim_date, amount FROM claims").fetchall()
    con.close()
    a, b = _summ(legacy), _summ(lake)
    months = []
    for m in sorted(set(a) | set(b)):
        x, y = a.get(m, {"rows": 0, "amount": 0, "xor": 0}), b.get(m, {"rows": 0, "amount": 0, "xor": 0})
        months.append({"month": m, "legacy_rows": x["rows"], "lake_rows": y["rows"],
                       "amount_diff": round(x["amount"] - y["amount"], 2), "checksum_match": x["xor"] == y["xor"]})
    lk, kk = {r[0] for r in legacy}, {r[0] for r in lake}
    return {"months": months, "only_in_legacy": sorted(lk - kk)[:20], "only_in_lake": sorted(kk - lk)[:20],
            "all_match": all(m["checksum_match"] and m["legacy_rows"] == m["lake_rows"] for m in months)}
