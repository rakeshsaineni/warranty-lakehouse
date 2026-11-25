"""A dealer-management OLTP database and its change stream, Debezium-style.

The source (SQLite) receives inserts, updates and deletes over time. Every change is
emitted as a CDC event {op, before, after, source: {table, lsn, ts_ms}}. The delivered
stream is then made realistic: ~3% duplicates (at-least-once delivery), ~5% of events
delivered out of order, and a schema change partway through (a new column).
"""

from __future__ import annotations

import json
import random
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

MODELS = ["Aster", "Birch", "Cedar"]
COMPONENTS = ["brake caliper", "fuel pump", "window regulator", "infotainment unit", "coolant pump"]
REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
SCHEMA = """
CREATE TABLE dealers (dealer_id TEXT PRIMARY KEY, name TEXT, region TEXT);
CREATE TABLE vehicles (vin TEXT PRIMARY KEY, model TEXT, model_year INTEGER, in_service_date TEXT);
CREATE TABLE claims (claim_id TEXT PRIMARY KEY, vin TEXT, dealer_id TEXT, component TEXT, status TEXT,
                     amount REAL, claim_date TEXT, labor_hours REAL);
"""


class Source:
    def __init__(self, path: Path | str = ":memory:"):
        self.db = sqlite3.connect(str(path))
        self.db.executescript(SCHEMA)
        self.lsn = 0
        self.events: list[dict] = []

    def _emit(self, table: str, op: str, before: dict | None, after: dict | None, ts: datetime) -> None:
        self.lsn += 1
        self.events.append({"op": op, "before": before, "after": after,
                            "source": {"table": table, "lsn": self.lsn, "ts_ms": int(ts.timestamp() * 1000)}})

    def _row(self, table: str, pk: str, key: str) -> dict | None:
        cur = self.db.execute(f"SELECT * FROM {table} WHERE {pk} = ?", (key,))
        r = cur.fetchone()
        return dict(zip([d[0] for d in cur.description], r)) if r else None

    def insert(self, table: str, pk: str, row: dict, ts: datetime, emit_cols: list[str] | None = None) -> None:
        cols = list(row)
        self.db.execute(f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
                        [row[c] for c in cols])
        after = self._row(table, pk, row[pk])
        if emit_cols is not None:
            after = {k: after[k] for k in emit_cols}
        self._emit(table, "c", None, after, ts)

    def update(self, table: str, pk: str, key: str, changes: dict, ts: datetime,
               emit_cols: list[str] | None = None) -> None:
        before = self._row(table, pk, key)
        self.db.execute(f"UPDATE {table} SET {','.join(f'{c} = ?' for c in changes)} WHERE {pk} = ?",
                        [*changes.values(), key])
        after = self._row(table, pk, key)
        if emit_cols is not None:
            before, after = ({k: x[k] for k in emit_cols} for x in (before, after))
        self._emit(table, "u", before, after, ts)

    def delete(self, table: str, pk: str, key: str, ts: datetime) -> None:
        before = self._row(table, pk, key)
        self.db.execute(f"DELETE FROM {table} WHERE {pk} = ?", (key,))
        self._emit(table, "d", before, None, ts)

    def snapshot(self, table: str) -> list[dict]:
        cur = self.db.execute(f"SELECT * FROM {table}")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def simulate(n_vehicles: int = 3000, n_claims: int = 6000, seed: int = 4) -> tuple[Source, list[dict]]:
    """Run the source system and return (source, delivered_events)."""
    rng, src = random.Random(seed), Source()
    t = datetime(2026, 1, 1)
    claim_cols_v1 = ["claim_id", "vin", "dealer_id", "component", "status", "amount", "claim_date"]
    for i in range(40):
        src.insert("dealers", "dealer_id", {"dealer_id": f"D{i:03d}", "name": f"Dealer {i}",
                                            "region": rng.choice(REGIONS)}, t)
    for i in range(n_vehicles):
        src.insert("vehicles", "vin", {"vin": f"1DEMO{i:012d}", "model": rng.choice(MODELS),
                                       "model_year": rng.choice([2025, 2026]),
                                       "in_service_date": (date(2025, 6, 1) + timedelta(days=rng.randint(0, 200)))
                                       .isoformat()}, t)
    open_claims: list[str] = []
    schema_change_at = n_claims // 2
    for i in range(n_claims):
        t += timedelta(minutes=rng.randint(1, 90))
        v2 = i >= schema_change_at  # labor_hours starts flowing halfway through: schema evolution
        cid = f"C{i:06d}"
        row = {"claim_id": cid, "vin": f"1DEMO{rng.randrange(n_vehicles):012d}",
               "dealer_id": f"D{rng.randrange(40):03d}",
               "component": rng.choice(COMPONENTS), "status": "submitted", "amount": round(rng.uniform(80, 2400), 2),
               "claim_date": t.date().isoformat()}
        if v2:
            row["labor_hours"] = round(rng.uniform(0.5, 8), 1)
        src.insert("claims", "claim_id", row, t, None if v2 else claim_cols_v1)
        open_claims.append(cid)
        if rng.random() < 0.5 and open_claims:  # lifecycle updates on an earlier claim
            c = rng.choice(open_claims)
            cur = src._row("claims", "claim_id", c)
            nxt = {"submitted": "approved", "approved": "paid"}.get(cur["status"])
            if nxt:
                changes = {"status": nxt}
                if nxt == "approved" and rng.random() < 0.3:
                    changes["amount"] = round(cur["amount"] * rng.uniform(0.7, 1.0), 2)  # adjusted on review
                src.update("claims", "claim_id", c, changes, t, None if v2 else claim_cols_v1)
        if rng.random() < 0.03 and open_claims:  # voided claims are deleted
            c = open_claims.pop(rng.randrange(len(open_claims)))
            src.delete("claims", "claim_id", c, t)
        if rng.random() < 0.01:  # dealer moves to another region (SCD2 in the warehouse)
            d = f"D{rng.randrange(40):03d}"
            src.update("dealers", "dealer_id", d, {"region": rng.choice(REGIONS)}, t)
    delivered = []
    for e in src.events:  # at-least-once delivery: some events arrive twice
        delivered.append(e)
        if rng.random() < 0.03:
            delivered.append(json.loads(json.dumps(e)))
    for _ in range(int(len(delivered) * 0.05)):  # local reordering, like partitions and retries produce
        i = rng.randrange(len(delivered) - 5)
        j = i + rng.randint(1, 5)
        delivered[i], delivered[j] = delivered[j], delivered[i]
    return src, delivered
