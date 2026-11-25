import shutil

import pytest

from lakehouse.migration import export_legacy_csv, reconcile
from lakehouse.pipeline import build_silver, check, compare_with_source, load_contracts, to_bronze
from lakehouse.report import corrupt, naive_apply
from lakehouse.source import simulate


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    src, delivered = simulate(n_vehicles=500, n_claims=1500, seed=9)
    to_bronze(delivered, root, load_contracts())
    build_silver(root, root / "wh.duckdb")
    return root, src, delivered


def test_silver_equals_source_despite_duplicates_and_reordering(built):
    root, src, delivered = built
    assert len(delivered) > len(src.events)  # duplicates were delivered
    for table, r in compare_with_source(root / "wh.duckdb", src).items():
        assert r["missing"] == r["unexpected"] == 0, table


def test_replay_is_harmless_for_lsn_pipeline_but_not_for_naive(built, tmp_path):
    root, src, delivered = built
    replayed = delivered + delivered[: len(delivered) // 3]
    to_bronze(replayed, tmp_path, load_contracts())
    build_silver(tmp_path, tmp_path / "wh.duckdb")
    assert all(r["missing"] == 0 for r in compare_with_source(tmp_path / "wh.duckdb", src).values())
    truth = {r["claim_id"] for r in src.snapshot("claims")}
    assert set(naive_apply(replayed)["claims"]) != truth  # deleted claims come back to life


def test_contracts_quarantine_bad_events_and_allow_new_nullable_column(tmp_path, built):
    _, _, delivered = built
    c = load_contracts()
    row = {"claim_id": "C1", "vin": "V", "dealer_id": "D", "component": "x", "status": "paid", "amount": 10.0,
           "claim_date": "2026-03-01"}
    assert check(row, c["claims"]) == []
    assert check({**row, "labor_hours": 2.5}, c["claims"]) == []  # schema v2: additive change
    assert "amount: expected number" in check({**row, "amount": "1,200.00"}, c["claims"])[0]
    assert "unexpected column" in check({**row, "warranty_code": "X"}, c["claims"])[0]
    assert to_bronze(corrupt(delivered, 10), tmp_path, c)["quarantined"] == 10


def test_scd2_has_one_current_version_per_dealer(built):
    import duckdb

    con = duckdb.connect(str(built[0] / "wh.duckdb"), read_only=True)
    assert con.execute("SELECT count(*) FROM dealers_history WHERE is_current").fetchone()[0] == 40
    gaps = con.execute("""SELECT count(*) FROM (SELECT valid_to, lead(valid_from) OVER (PARTITION BY dealer_id
                          ORDER BY valid_from) AS nxt FROM dealers_history) WHERE nxt IS NOT NULL AND nxt <> valid_to
                       """).fetchone()[0]
    assert gaps == 0  # versions are contiguous


def test_reconciliation_detects_a_missing_claim(built, tmp_path):
    root, src, _ = built
    rows = src.snapshot("claims")
    export_legacy_csv(rows, tmp_path / "ok.csv")
    assert reconcile(tmp_path / "ok.csv", root / "wh.duckdb")["all_match"]
    export_legacy_csv(rows + [{**rows[0], "claim_id": "C-LEGACY-ONLY"}], tmp_path / "bad.csv")
    rep = reconcile(tmp_path / "bad.csv", root / "wh.duckdb")
    assert not rep["all_match"] and rep["only_in_legacy"] == ["C-LEGACY-ONLY"]


@pytest.mark.skipif(shutil.which("java") is None, reason="Spark needs a JVM")
def test_spark_and_duckdb_produce_identical_silver(built):
    import duckdb

    from lakehouse.spark_silver import silver_claims, spark_session

    spark = spark_session()
    try:
        got = {(r.claim_id, r.status, round(r.amount, 2)) for r in silver_claims(spark, built[0] / "bronze").collect()}
    finally:
        spark.stop()
    con = duckdb.connect(str(built[0] / "wh.duckdb"), read_only=True)
    want = {(a, b, round(c, 2)) for a, b, c in con.execute("SELECT claim_id, status, amount FROM claims").fetchall()}
    assert got == want


def test_dag_structure():
    """Checks the DAG wiring without installing Airflow (the scheduler image has it)."""
    import ast
    from pathlib import Path

    src = Path(__file__).resolve().parents[1] / "dags" / "warranty_lakehouse.py"
    tree = ast.parse(src.read_text())
    tasks = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert {"ingest_cdc_to_bronze", "build_silver", "quarantine_report"} <= tasks
    assert "silver >> dbt_build >> quarantine_report()" in src.read_text()
