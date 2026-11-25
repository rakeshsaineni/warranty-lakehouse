"""End-to-end run with measurements -> reports/report.md"""

from __future__ import annotations

import json
import random
import shutil
import time
from pathlib import Path

import duckdb

from .migration import export_legacy_csv, reconcile
from .pipeline import build_silver, compare_with_source, load_contracts, to_bronze
from .source import simulate


def naive_apply(events: list[dict]) -> dict[str, dict]:
    """What a first-version consumer often does: apply events in arrival order, no dedupe, no LSN check."""
    state: dict[str, dict] = {"claims": {}, "vehicles": {}}
    for e in events:
        t = e["source"]["table"]
        if t not in state:
            continue
        pk = "claim_id" if t == "claims" else "vin"
        if e["op"] == "d":
            state[t].pop(e["before"][pk], None)
        else:
            state[t][e["after"][pk]] = e["after"]
    return state


def corrupt(events: list[dict], n: int, seed: int = 1) -> list[dict]:
    """Simulate an upstream bug: some claim amounts arrive as formatted strings, some statuses are unknown."""
    rng = random.Random(seed)
    out = json.loads(json.dumps(events))
    claims = [e for e in out if e["source"]["table"] == "claims" and e["op"] in ("c", "u")]
    for e in rng.sample(claims, n):
        if rng.random() < 0.5:
            e["after"]["amount"] = f"{e['after']['amount']:,.2f}"
        else:
            e["after"]["status"] = "pending_review"
    return out


def run(root: Path = Path("data"), out: Path = Path("reports"), scale: int = 1) -> dict:
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    src, delivered = simulate(n_vehicles=3000 * scale, n_claims=6000 * scale)
    contracts = load_contracts()
    t0 = time.perf_counter()
    landed = to_bronze(delivered, root, contracts)
    silver = build_silver(root, root / "warehouse.duckdb")
    elapsed = time.perf_counter() - t0
    exact = compare_with_source(root / "warehouse.duckdb", src)

    truth = {r["claim_id"]: r for r in src.snapshot("claims")}

    def score_naive(events):
        naive = naive_apply(events)["claims"]
        stale = sum(1 for k, v in truth.items() if k in naive and
                    (naive[k]["status"], naive[k]["amount"]) != (v["status"], v["amount"]))
        return {"stale_claims": stale, "deleted_claims_still_present": sum(1 for k in naive if k not in truth)}

    # A consumer crash or offset reset replays the first 30% of the topic after newer events
    replayed = delivered + delivered[: int(len(delivered) * 0.3)]
    rp_root = root / "replay"
    to_bronze(replayed, rp_root, contracts)
    build_silver(rp_root, rp_root / "wh.duckdb")
    replay_exact = compare_with_source(rp_root / "wh.duckdb", src)

    bad_root = root / "corrupted"
    q = to_bronze(corrupt(delivered, 40), bad_root, contracts)
    reasons = [r[0] for r in duckdb.sql(
        f"SELECT errors FROM read_parquet('{bad_root.as_posix()}/quarantine/*.parquet')").fetchall()]

    export_legacy_csv(src.snapshot("claims"), root / "legacy_claims.csv")
    rec = reconcile(root / "legacy_claims.csv", root / "warehouse.duckdb")
    result = {"events_delivered": len(delivered), "unique_events": silver["events"],
              "duplicates": len(delivered) - silver["events"], "bronze_plus_silver_seconds": round(elapsed, 3),
              "events_per_second": int(len(delivered) / elapsed), "silver_vs_source": exact,
              "naive_consumer": {"normal_delivery": score_naive(delivered), "after_replay": score_naive(replayed)},
              "lsn_pipeline_after_replay": replay_exact,
              "quarantine": {"corrupted_events": 40, "quarantined": q["quarantined"],
                             "wrong_type_amount": sum("amount" in r for r in reasons),
                             "unknown_status": sum("status" in r for r in reasons)},
              "reconciliation": {"months": len(rec["months"]), "all_match": rec["all_match"]}, "landed": landed}
    out.mkdir(exist_ok=True)
    (out / "report.json").write_text(json.dumps(result, indent=1))
    return result
