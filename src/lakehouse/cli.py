"""lakehouse run [--scale N] -- simulate CDC, build bronze/silver, verify, write reports/report.json"""

from __future__ import annotations

import argparse
import json
import sys

from .report import run


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="lakehouse")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--scale", type=int, default=1)
    a = ap.parse_args(argv)
    result = run(scale=a.scale)
    print(json.dumps(result, indent=1))
    exact = all(v["missing"] == 0 and v["unexpected"] == 0 for v in result["silver_vs_source"].values())
    if not exact or not result["reconciliation"]["all_match"]:
        print("GATE FAILED: silver does not match the source system", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
