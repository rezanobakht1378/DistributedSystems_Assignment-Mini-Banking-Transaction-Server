"""Condense a k8s_experiments.py report into one block per scenario."""

import json
import sys

report = json.load(open(sys.argv[1], encoding="utf-8"))
for name, result in report["results"].items():
    details = {k: v for k, v in result["details"].items() if k not in {"timeline", "controller_output"}}
    load = [
        {k: item[k] for k in ("operations", "cross_shard_operations", "outcomes", "client_retries",
                              "seconds_with_errors_and_no_success", "max_seconds_to_definitive_answer")}
        for item in result["load"]
    ]
    invariants = result["invariants"]
    print(json.dumps({
        "scenario": name,
        "passed": result["passed"],
        "details": details,
        "load": load,
        "checks_failed": [k for k, ok in invariants["checks"].items() if not ok],
        "db": {k: invariants.get(k) for k in (
            "distinct_requests", "ledger_rows_verified", "total_before", "acknowledged_deposits",
            "acknowledged_withdrawals", "total_expected", "total_actual", "coordinator_states",
            "participant_rows", "problems")},
    }, indent=1))
