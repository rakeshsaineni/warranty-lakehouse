"""Bronze (raw CDC in Parquet) -> contract checks -> silver (current state + SCD2) in DuckDB.

Correctness under real delivery conditions comes from two rules:
  * dedupe on (table, lsn): at-least-once duplicates disappear
  * last write wins by lsn, not by arrival: out-of-order delivery doesn't matter
"""

from __future__ import annotations

import json
import time
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

ROOT = Path(__file__).resolve().parents[2]
PK = {"claims": "claim_id", "vehicles": "vin", "dealers": "dealer_id"}


def load_contracts(folder: Path = ROOT / "contracts") -> dict[str, dict]:
    return {c["table"]: c for c in (yaml.safe_load(p.read_text()) for p in sorted(folder.glob("*.yaml")))}


def check(row: dict, contract: dict) -> list[str]:
    errs = []
    cols = contract["columns"]
    for col in row:
        if col not in cols:
            errs.append(f"unexpected column {col} (contract v{contract['version']})")
    for col, spec in cols.items():
        v = row.get(col)
        if v is None:
            if spec.get("required"):
                errs.append(f"{col} is required")
            continue
        t = spec["type"]
        ok = {"string": isinstance(v, str), "integer": isinstance(v, int) and not isinstance(v, bool),
              "number": isinstance(v, (int, float)) and not isinstance(v, bool)}.get(t)
        if t == "date":
            try:
                date.fromisoformat(v)
                ok = True
            except (TypeError, ValueError):
                ok = False
        if not ok:
            errs.append(f"{col}: expected {t}, got {type(v).__name__} {v!r}")
            continue
        if "allowed" in spec and v not in spec["allowed"]:
            errs.append(f"{col}: {v!r} not in {spec['allowed']}")
        if "min" in spec and v < spec["min"]:
            errs.append(f"{col}: {v} < {spec['min']}")
    return errs


def to_bronze(events: list[dict], root: Path, contracts: dict[str, dict], batch_id: str = "0") -> dict:
    """Validate and land events. Valid ones go to bronze/<table>/, invalid ones to quarantine/."""
    good, bad = [], []
    now = int(time.time() * 1000)
    for e in events:
        table = e["source"]["table"]
        row = e["after"] if e["op"] != "d" else e["before"]
        errs = check(row, contracts[table]) if e["op"] != "d" else []
        rec = {"table": table, "op": e["op"], "lsn": e["source"]["lsn"], "ts_ms": e["source"]["ts_ms"],
               "pk": str(row[PK[table]]), "payload": json.dumps(row), "ingested_ms": now}
        (bad if errs else good).append({**rec, "errors": "; ".join(errs)} if errs else rec)
    for name, rows in (("bronze", good), ("quarantine", bad)):
        if rows:
            path = root / name / f"batch-{batch_id}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(rows), path)
    return {"landed": len(good), "quarantined": len(bad)}


SILVER_SQL = """
CREATE OR REPLACE VIEW bronze AS SELECT * FROM read_parquet('{bronze}/*.parquet');
CREATE OR REPLACE TABLE events AS
  SELECT * FROM bronze QUALIFY row_number() OVER (PARTITION BY "table", lsn ORDER BY ingested_ms) = 1;
CREATE OR REPLACE TABLE latest AS
  SELECT * FROM events QUALIFY row_number() OVER (PARTITION BY "table", pk ORDER BY lsn DESC) = 1;

CREATE OR REPLACE TABLE claims AS
SELECT payload->>'claim_id' AS claim_id, payload->>'vin' AS vin, payload->>'dealer_id' AS dealer_id,
       payload->>'component' AS component, payload->>'status' AS status,
       CAST(payload->>'amount' AS DOUBLE) AS amount, CAST(payload->>'claim_date' AS DATE) AS claim_date,
       CAST(payload->>'labor_hours' AS DOUBLE) AS labor_hours, lsn AS _lsn
FROM latest WHERE "table" = 'claims' AND op <> 'd';

CREATE OR REPLACE TABLE vehicles AS
SELECT payload->>'vin' AS vin, payload->>'model' AS model, CAST(payload->>'model_year' AS INTEGER) AS model_year,
       CAST(payload->>'in_service_date' AS DATE) AS in_service_date, lsn AS _lsn
FROM latest WHERE "table" = 'vehicles' AND op <> 'd';

-- SCD Type 2: every version of a dealer, with the interval during which it was true
CREATE OR REPLACE TABLE dealers_history AS
SELECT payload->>'dealer_id' AS dealer_id, payload->>'name' AS name, payload->>'region' AS region,
       to_timestamp(ts_ms / 1000) AS valid_from,
       lead(to_timestamp(ts_ms / 1000)) OVER w AS valid_to,
       lead(lsn) OVER w IS NULL AS is_current, lsn AS _lsn
FROM events WHERE "table" = 'dealers' AND op <> 'd'
WINDOW w AS (PARTITION BY pk ORDER BY lsn);
"""


def build_silver(root: Path, db_path: Path) -> dict:
    t0 = time.perf_counter()
    con = duckdb.connect(str(db_path))
    con.execute(SILVER_SQL.format(bronze=(root / "bronze").as_posix()))
    counts = {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
              for t in ("events", "claims", "vehicles", "dealers_history")}
    counts["raw_events"] = con.execute("SELECT count(*) FROM bronze").fetchone()[0]
    con.close()
    return {**counts, "seconds": round(time.perf_counter() - t0, 3)}


def compare_with_source(db_path: Path, source) -> dict[str, dict]:
    """Row-level check that silver equals the source system's final state."""
    con = duckdb.connect(str(db_path), read_only=True)
    out = {}
    for table, cols in (("claims", ["claim_id", "vin", "dealer_id", "component", "status", "amount", "labor_hours"]),
                        ("vehicles", ["vin", "model", "model_year"])):
        src = {tuple(r[c] for c in cols) for r in source.snapshot(table)}
        got = {tuple(r) for r in con.execute(f"SELECT {', '.join(cols)} FROM {table}").fetchall()}
        out[table] = {"source_rows": len(src), "silver_rows": len(got), "missing": len(src - got),
                      "unexpected": len(got - src)}
    cur = {(r["dealer_id"], r["region"]) for r in source.snapshot("dealers")}
    got = set(con.execute("SELECT dealer_id, region FROM dealers_history WHERE is_current").fetchall())
    out["dealers (current SCD2 row)"] = {"source_rows": len(cur), "silver_rows": len(got),
                                         "missing": len(cur - got), "unexpected": len(got - cur)}
    con.close()
    return out
