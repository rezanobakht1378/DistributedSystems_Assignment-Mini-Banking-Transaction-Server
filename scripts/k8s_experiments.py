"""Kubernetes experiments for the banking platform.

Every scenario runs transfer/deposit load through the public gateway while a
disruption happens (Pod kills, DB failover, rolling update, ...). Clients follow
the platform contract: on a network error or 5xx they retry with the same
X-Request-ID until they get a definitive answer. Afterwards the run is checked
against the databases (read on the current primaries), not only HTTP answers:

* every operation got a definitive answer (nothing left unresolved),
* the ledger matches the answers exactly: every acknowledged operation has its
  rows (deposit/withdrawal 1, same-shard transfer 2, cross-shard transfer 1 per
  shard), rejected/aborted ones have none, and nothing is applied twice,
* money is conserved: sum(balances) == before + deposits - withdrawals,
* each account's ledger is an unbroken chain ending at its stored balance,
* every cross-shard transfer is COMMITTED on both participants or on neither,
  and no coordinator or participant state is left unresolved.

The seven required failure experiments are `--scenarios required`.
"""

from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from pathlib import Path
from typing import Any, Callable

import httpx


ROOT = Path(__file__).resolve().parents[1]
SHARD_ACCOUNTS = {
    "shard-a": ("A0001", "A0002", "A0003", "A0004"),
    "shard-b": ("A1001", "A1002", "A1003", "A1004"),
}
ACCOUNTS = SHARD_ACCOUNTS["shard-a"] + SHARD_ACCOUNTS["shard-b"]
# Accounts the background load never touches, so scenario-specific operations
# on them have exactly predictable results.
RESERVED_ACCOUNTS = ("A0005", "A1005")
TRACKED_ACCOUNTS = ACCOUNTS + RESERVED_ACCOUNTS
DATABASES = {
    "shard-a-db": "banking_shard_a",
    "shard-b-db": "banking_shard_b",
    "coordinator-db": "banking_coordinator",
}
APP_FOR_DATABASE = {
    "shard-a-db": "shard-a-app",
    "shard-b-db": "shard-b-app",
    "coordinator-db": "gateway",
}
DEPLOYMENTS = ("shard-a-app", "shard-b-app", "gateway")
MINIMUM_FUNDS = Decimal("50.00")
REQUIRED_SCENARIOS = (
    "pod-failures",                  # 1 application Pod failure
    "db-failover:shard-a-db",        # 2 primary database failure (shard)
    "db-failover:coordinator-db",    # 2 primary database failure (coordinator)
    "concurrent-transactions",       # 3 concurrent transactions
    "coordinator-interruption",      # 4 coordinator interruption
    "autoscale",                     # 5 autoscaling under load
    "rolling-update",                # 6 rolling update under load
    "repeated-requests",             # 7 repeated client requests
)
EXTRA_SCENARIOS = (
    "network-policy",
    "baseline",
    "db-pod-delete:shard-b-db",
    "db-switchover:coordinator-db",
    "rollback",
    "backup",
)
PRESETS = {
    "required": REQUIRED_SCENARIOS,
    "all": EXTRA_SCENARIOS[:2] + REQUIRED_SCENARIOS + EXTRA_SCENARIOS[2:],
}


def shard_of(account: str) -> str:
    return "shard-a" if int(account[1:]) <= 1000 else "shard-b"


def error_code(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return "invalid_json"
    if isinstance(body, dict):
        detail = body.get("detail")
        if isinstance(detail, dict) and "error" in detail:
            return str(detail["error"])
        if "error" in body:
            return str(body["error"])
    return "unknown"


class Kubernetes:
    def __init__(self, namespace: str, profile: str, context: str | None = None) -> None:
        self.namespace = namespace
        self.profile = profile
        self.context = ["--context", context] if context else []

    def run(self, *arguments: str, check: bool = True, timeout: float = 180) -> str:
        try:
            result = subprocess.run(
                ["kubectl", *self.context, "-n", self.namespace, *arguments],
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired:
            if check:
                raise RuntimeError(f"kubectl {' '.join(arguments)} timed out after {timeout:.0f} s")
            return ""
        if check and result.returncode:
            raise RuntimeError(
                f"kubectl {' '.join(arguments)} failed: "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )
        return result.stdout.strip()

    def json(self, *arguments: str) -> Any:
        return json.loads(self.run(*arguments, "-o", "json"))

    def node_ssh(self, node: str, command: str, check: bool = True) -> str:
        # Minikube nodes (docker driver) are containers named after the node;
        # docker exec keeps working when the control plane is gone.
        result = subprocess.run(
            ["docker", "exec", node, "sh", "-c", command],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        if check and result.returncode:
            raise RuntimeError(
                f"command {command!r} on node {node} failed: "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )
        return result.stdout.strip()

    def leader(self, cluster: str) -> tuple[str, int]:
        lease = self.json("get", "lease", f"{cluster}-leader")
        return lease["spec"]["holderIdentity"], int(lease["spec"].get("leaseTransitions") or 0)

    def psql(self, cluster: str, sql: str) -> list[list[str]]:
        pod, _ = self.leader(cluster)
        output = self.run(
            "exec", pod, "-c", "postgres", "--",
            "psql", "-U", "banking", "-d", DATABASES[cluster],
            "-v", "ON_ERROR_STOP=1", "-At", "-F", "\t", "-c", sql,
        )
        return [line.split("\t") for line in output.splitlines() if line]

    def pods(self, name: str) -> list[dict[str, Any]]:
        return self.json("get", "pods", "-l", f"app.kubernetes.io/name={name}")["items"]

    @staticmethod
    def is_ready(pod: dict[str, Any]) -> bool:
        if pod["metadata"].get("deletionTimestamp"):
            return False
        return any(
            condition["type"] == "Ready" and condition["status"] == "True"
            for condition in pod.get("status", {}).get("conditions", [])
        )

    def ready_pods(self, name: str) -> list[str]:
        return sorted(pod["metadata"]["name"] for pod in self.pods(name) if self.is_ready(pod))

    def restarts(self, name: str) -> dict[str, int]:
        return {
            pod["metadata"]["name"]: sum(
                status.get("restartCount", 0)
                for status in pod.get("status", {}).get("containerStatuses", [])
            )
            for pod in self.pods(name)
        }

    def deployment_settled(self, name: str) -> bool:
        deployment = self.json("get", "deployment", name)
        spec = deployment["spec"].get("replicas", 1)
        status = deployment.get("status", {})
        return (
            status.get("observedGeneration") == deployment["metadata"]["generation"]
            and status.get("updatedReplicas", 0) == spec
            and status.get("readyReplicas", 0) == spec
            and status.get("replicas", 0) == spec
        )

    def wait_deployments(self, names: tuple[str, ...], timeout: float = 180) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if all(self.deployment_settled(name) for name in names):
                return
            time.sleep(2)
        raise TimeoutError(f"Deployments did not settle: {names}")

    def database_status(self) -> dict[str, Any]:
        output = self.run("exec", "deploy/db-controller", "--", "python", "db_controller.py", "status")
        return {entry["cluster"]: entry for entry in json.loads(output[output.index("["):])}

    def replication_healthy(self, cluster: str) -> bool:
        try:
            status = self.database_status()[cluster]
        except (RuntimeError, ValueError, KeyError):
            return False
        leader = status["pods"].get(status["leader"], {})
        streaming = {
            replica["name"] for replica in leader.get("replicas", [])
            if replica.get("state") == "streaming"
        }
        members = [name for name in status["pods"] if name != status["leader"]]
        return (
            leader.get("reachable") is True
            and leader.get("role") == "primary"
            and any(replica.get("sync_state") == "sync" for replica in leader.get("replicas", []))
            and bool(members)
            and all(
                status["pods"][name].get("role") == "standby" and name in streaming
                for name in members
            )
        )

    def wait_replication(self, cluster: str, timeout: float = 240) -> float:
        started = time.monotonic()
        while time.monotonic() - started < timeout:
            if self.replication_healthy(cluster):
                return round(time.monotonic() - started, 1)
            time.sleep(3)
        raise TimeoutError(f"{cluster} did not return to primary + synchronous standby.")

    def container_task(self, pod_name: str, container: str) -> tuple[str, str]:
        pod = self.json("get", "pod", pod_name)
        for status in pod["status"].get("containerStatuses", []):
            if status["name"] == container and status.get("containerID"):
                return pod["spec"]["nodeName"], status["containerID"].split("://", 1)[1]
        raise RuntimeError(f"{pod_name}/{container} has no running container.")

    def freeze(self, node: str, container_id: str) -> None:
        self.node_ssh(node, f"sudo ctr -n k8s.io task pause {container_id}")

    def thaw(self, node: str, container_id: str) -> None:
        self.node_ssh(node, f"sudo ctr -n k8s.io task resume {container_id}", check=False)


@dataclass
class Operation:
    request_id: str
    kind: str
    account: str
    amount: Decimal
    destination: str | None = None
    attempts: int = 0
    outcome: str | None = None
    status_code: int | None = None
    started: float = 0.0
    finished: float | None = None
    response_body: Any = None
    gateway_replica: str | None = None

    @property
    def cross_shard(self) -> bool:
        return self.destination is not None and shard_of(self.account) != shard_of(self.destination)

    def expected_ledger_rows(self) -> dict[str, int]:
        """Ledger rows per shard that this operation must leave behind."""
        if self.outcome != "success":
            return {}
        if self.kind in {"deposit", "withdraw"}:
            return {shard_of(self.account): 1}
        if self.cross_shard:
            return {shard_of(self.account): 1, shard_of(self.destination): 1}
        return {shard_of(self.account): 2}

    def body(self) -> dict[str, str]:
        body = {"action": self.kind, "account": self.account, "amount": str(self.amount)}
        if self.destination:
            body["destination"] = self.destination
        return body


class LoadGenerator:
    """Concurrent clients that retry every operation with its original ID."""

    def __init__(
        self,
        gateway: str,
        run_id: str,
        concurrency: int,
        settle_timeout: float,
        cross_shard_ratio: float = 0.5,
        deposit_ratio: float = 0.2,
        seed: int = 0,
    ) -> None:
        self.gateway = gateway
        self.run_id = run_id
        self.concurrency = concurrency
        self.settle_timeout = settle_timeout
        self.cross_shard_ratio = cross_shard_ratio
        self.deposit_ratio = deposit_ratio
        self.seed = seed
        self.client = httpx.Client(
            timeout=25,
            limits=httpx.Limits(
                max_connections=max(concurrency, 32) * 2,
                max_keepalive_connections=max(concurrency, 32),
            ),
        )
        self.operations: list[Operation] = []
        self.failures: list[tuple[float, str]] = []
        self.successes: list[float] = []
        self.backends: Counter[str] = Counter()
        self.gateways: Counter[str] = Counter()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._settle_deadline = float("inf")
        self._threads: list[threading.Thread] = []
        self.started_at = 0.0

    def start(self) -> None:
        self.started_at = time.monotonic()
        for index in range(self.concurrency):
            thread = threading.Thread(target=self._worker, args=(index,), daemon=True)
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        self._settle_deadline = time.monotonic() + self.settle_timeout
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=self.settle_timeout + 30)
        self.client.close()

    def _next_operation(self, rng: random.Random, worker: int, sequence: int) -> Operation:
        request_id = f"{self.run_id}-w{worker}-{sequence}"
        account = rng.choice(ACCOUNTS)
        if rng.random() < self.deposit_ratio:
            amount = Decimal(rng.randint(1, 10)) / 100
            return Operation(request_id, "deposit", account, amount)
        shard = shard_of(account)
        if rng.random() < self.cross_shard_ratio:
            other = "shard-b" if shard == "shard-a" else "shard-a"
            destination = rng.choice(SHARD_ACCOUNTS[other])
        else:
            destination = rng.choice([item for item in SHARD_ACCOUNTS[shard] if item != account])
        amount = Decimal(rng.randint(1, 50)) / 100
        return Operation(request_id, "transfer", account, amount, destination)

    def _worker(self, worker: int) -> None:
        rng = random.Random(self.seed * 1000 + worker)
        sequence = 0
        while not self._stop.is_set():
            operation = self._next_operation(rng, worker, sequence)
            sequence += 1
            with self._lock:
                self.operations.append(operation)
            self.execute(operation)

    def execute_new(
        self,
        request_id: str,
        kind: str,
        account: str,
        amount: Decimal,
        destination: str | None = None,
    ) -> Operation:
        operation = Operation(request_id, kind, account, amount, destination)
        with self._lock:
            self.operations.append(operation)
        self.execute(operation)
        return operation

    def run_simultaneously(self, operations: list[Operation]) -> None:
        """Release all operations at the same instant and wait for their answers."""
        if not self.started_at:
            self.started_at = time.monotonic()
        with self._lock:
            self.operations.extend(operations)
        barrier = threading.Barrier(len(operations))

        def run(operation: Operation) -> None:
            barrier.wait(timeout=30)
            self.execute(operation)

        with ThreadPoolExecutor(max_workers=len(operations)) as pool:
            list(pool.map(run, operations))

    def execute(self, operation: Operation) -> None:
        if not self.started_at:
            self.started_at = time.monotonic()
        operation.started = time.monotonic()
        backoff = 0.2
        while True:
            operation.attempts += 1
            reason = None
            try:
                response = self.client.post(
                    self.gateway + "/transactions",
                    json=operation.body(),
                    headers={"X-Request-ID": operation.request_id},
                )
            except httpx.HTTPError as error:
                reason = type(error).__name__
            else:
                if response.status_code < 500:
                    operation.status_code = response.status_code
                    code = "success" if response.is_success else error_code(response)
                    operation.outcome = {
                        "success": "success",
                        "transaction_aborted": "aborted",
                        "insufficient_funds": "rejected",
                    }.get(code, f"unexpected:{response.status_code}:{code}")
                    try:
                        operation.response_body = response.json()
                    except ValueError:
                        operation.response_body = response.text
                    operation.gateway_replica = response.headers.get("X-Gateway-Replica")
                    operation.finished = time.monotonic()
                    with self._lock:
                        self.successes.append(operation.finished - self.started_at)
                        self.backends[response.headers.get("X-Backend-Replica", "coordinator")] += 1
                        self.gateways[response.headers.get("X-Gateway-Replica", "?")] += 1
                    return
                reason = f"http_{response.status_code}:{error_code(response)}"
            with self._lock:
                self.failures.append((time.monotonic() - self.started_at, reason))
            if self._stop.is_set() and time.monotonic() > self._settle_deadline:
                return
            time.sleep(backoff)
            backoff = min(backoff * 2, 2.0)

    def summary(self) -> dict[str, Any]:
        outcomes = Counter(operation.outcome or "unresolved" for operation in self.operations)
        waits = [
            operation.finished - operation.started
            for operation in self.operations
            if operation.finished is not None
        ]
        failure_seconds = {int(moment) for moment, _ in self.failures}
        success_seconds = {int(moment) for moment in self.successes}
        elapsed = max([*success_seconds, *failure_seconds, 0]) + 1
        acknowledged = sorted(self.successes)
        gaps = [(later - earlier, earlier) for earlier, later in zip(acknowledged, acknowledged[1:])]
        longest_gap, gap_start = max(gaps, default=(0.0, 0.0))
        return {
            "concurrency": self.concurrency,
            "operations": len(self.operations),
            "throughput_per_second": round(len(self.successes) / elapsed, 1),
            "outcomes": dict(outcomes),
            "cross_shard_operations": sum(op.cross_shard for op in self.operations),
            "client_retries": len(self.failures),
            "retry_reasons": dict(Counter(reason for _, reason in self.failures).most_common(8)),
            "seconds_with_errors": len(failure_seconds),
            "seconds_with_errors_and_no_success": len(failure_seconds - success_seconds),
            # Blocked commits wait instead of failing, so this also covers them.
            "longest_gap_between_acknowledgements": {
                "seconds": round(longest_gap, 1),
                "from_second": round(gap_start, 1),
            },
            "max_seconds_to_definitive_answer": round(max(waits, default=0.0), 2),
            "backend_replicas": dict(self.backends),
            "gateway_replicas": dict(self.gateways),
        }


class Experiments:
    def __init__(self, args: argparse.Namespace) -> None:
        self.gateway = args.gateway.rstrip("/")
        self.k8s = Kubernetes(args.namespace, args.profile, args.context)
        self.duration = args.duration
        self.concurrency = args.concurrency
        self.image = args.image
        self.settle_timeout = args.settle_timeout
        self.client = httpx.Client(timeout=15)
        self.session = uuid.uuid4().hex[:8]

    def close(self) -> None:
        self.client.close()

    def gateway_get(self, path: str, **params: str) -> httpx.Response:
        deadline = time.monotonic() + 60
        while True:
            try:
                response = self.client.get(self.gateway + path, params=params or None)
                if response.status_code < 500:
                    return response
            except httpx.HTTPError:
                pass
            if time.monotonic() > deadline:
                raise TimeoutError(f"GET {path} kept failing")
            time.sleep(1)

    def balances(self) -> dict[str, Decimal]:
        result = {}
        for account in TRACKED_ACCOUNTS:
            response = self.gateway_get("/balance/inquiry", account=account)
            response.raise_for_status()
            result[account] = Decimal(str(response.json()["balance"]))
        return result

    def fund_accounts(self) -> None:
        generator = LoadGenerator(self.gateway, f"k8s-{self.session}-fund", 1, self.settle_timeout)
        for account, balance in self.balances().items():
            if balance < MINIMUM_FUNDS:
                operation = Operation(
                    f"k8s-{self.session}-fund-{account}", "deposit", account, Decimal("100.00")
                )
                generator.execute(operation)
                if operation.outcome != "success":
                    raise RuntimeError(f"Could not fund {account}: {operation.outcome}")
        generator.client.close()

    # ----------------------------------------------------------- verification

    def verify(
        self,
        run_id: str,
        generators: list[LoadGenerator],
        before: dict[str, Decimal],
    ) -> dict[str, Any]:
        operations = [operation for generator in generators for operation in generator.operations]
        unresolved = [op.request_id for op in operations if op.outcome is None]
        unexpected = {op.request_id: op.outcome for op in operations if op.outcome and op.outcome.startswith("unexpected")}

        coordinator_expected = {
            op.request_id: {"success": "COMMITTED", "aborted": "ABORTED"}.get(op.outcome or "")
            for op in operations
            if op.cross_shard and op.outcome in {"success", "aborted"}
        }
        coordinator_states: dict[str, str] = {}
        deadline = time.monotonic() + 90
        while True:
            rows = self.k8s.psql(
                "coordinator-db",
                "SELECT transaction_id, state FROM bank_coordinated_transfers "
                f"WHERE transaction_id LIKE '{run_id}-%'",
            )
            coordinator_states = dict(rows)
            if all(state in {"COMMITTED", "ABORTED"} for state in coordinator_states.values()):
                break
            if time.monotonic() > deadline:
                break
            time.sleep(3)
        coordinator_mismatch = {
            tid: {"expected": state, "actual": coordinator_states.get(tid)}
            for tid, state in coordinator_expected.items()
            if coordinator_states.get(tid) != state
        }
        unterminated = {
            tid: state for tid, state in coordinator_states.items()
            if state not in {"COMMITTED", "ABORTED"}
        }

        # Operations repeated with the same X-Request-ID must all have received
        # the same answer; the request then counts once.
        by_request: dict[str, list[Operation]] = {}
        for operation in operations:
            if operation.outcome is not None:
                by_request.setdefault(operation.request_id, []).append(operation)
        inconsistent_repeats = {
            request_id: sorted({json.dumps(op.response_body, sort_keys=True) for op in ops})
            for request_id, ops in by_request.items()
            if len({json.dumps(op.response_body, sort_keys=True) for op in ops}) > 1
        }
        unique = {request_id: ops[0] for request_id, ops in by_request.items()}
        expected_rows: dict[str, dict[str, int]] = {"shard-a": {}, "shard-b": {}}
        for request_id, operation in unique.items():
            for shard, rows in operation.expected_ledger_rows().items():
                expected_rows[shard][request_id] = rows

        participants: dict[str, dict[str, str]] = {}
        duplicates = {}
        broken_chain = {}
        stale_balance = {}
        ledger_mismatch: dict[str, dict[str, Any]] = {}
        accounts_sql = ",".join(f"'{account}'" for account in TRACKED_ACCOUNTS)
        for cluster in ("shard-a-db", "shard-b-db"):
            actual_rows = {
                request_id: int(count)
                for request_id, count in self.k8s.psql(cluster, (
                    "SELECT request_id, count(*) FROM bank_transactions "
                    f"WHERE request_id LIKE '{run_id}-%' GROUP BY request_id"
                ))
            }
            shard = cluster.removesuffix("-db")
            for request_id in set(actual_rows) | set(expected_rows[shard]):
                expected = expected_rows[shard].get(request_id, 0)
                if actual_rows.get(request_id, 0) != expected:
                    ledger_mismatch[f"{shard}:{request_id}"] = {
                        "expected_rows": expected,
                        "actual_rows": actual_rows.get(request_id, 0),
                        "client_outcome": unique[request_id].outcome if request_id in unique else None,
                    }
            duplicates[cluster] = int(self.k8s.psql(cluster, (
                "SELECT count(*) FROM (SELECT 1 FROM bank_transactions "
                f"WHERE request_id LIKE '{run_id}-%' "
                "GROUP BY request_id, account, entry HAVING count(*) > 1) d"
            ))[0][0])
            broken_chain[cluster] = int(self.k8s.psql(cluster, (
                "SELECT count(*) FROM (SELECT balance_before, lag(balance_after) "
                "OVER (PARTITION BY account ORDER BY id) AS previous "
                f"FROM bank_transactions WHERE account IN ({accounts_sql})) t "
                "WHERE previous IS NOT NULL AND previous <> balance_before"
            ))[0][0])
            stale_balance[cluster] = int(self.k8s.psql(cluster, (
                "SELECT count(*) FROM accounts a JOIN LATERAL (SELECT balance_after "
                "FROM bank_transactions t WHERE t.account = a.wallet "
                "ORDER BY id DESC LIMIT 1) last ON true "
                f"WHERE a.wallet IN ({accounts_sql}) AND last.balance_after <> a.balance"
            ))[0][0])
            for transaction_id, role, state in self.k8s.psql(cluster, (
                "SELECT transaction_id, role, state FROM bank_prepared_transfers "
                f"WHERE transaction_id LIKE '{run_id}-%'"
            )):
                participants.setdefault(transaction_id, {})[role] = state
        open_prepared = sorted(
            transaction_id for transaction_id, roles in participants.items()
            if "prepared" in roles.values()
        )
        atomicity_violations = {}
        for transaction_id in set(coordinator_states) | set(participants):
            decision = coordinator_states.get(transaction_id)
            roles = participants.get(transaction_id, {})
            committed = [role for role, state in roles.items() if state == "committed"]
            if decision == "COMMITTED" and sorted(committed) != ["credit", "debit"]:
                atomicity_violations[transaction_id] = {"decision": decision, "participants": roles}
            elif decision != "COMMITTED" and committed:
                atomicity_violations[transaction_id] = {"decision": decision, "participants": roles}

        after = self.balances()

        def acknowledged(kind: str) -> Decimal:
            return sum(
                (op.amount for op in unique.values() if op.kind == kind and op.outcome == "success"),
                Decimal("0"),
            )
        deposits = acknowledged("deposit")
        withdrawals = acknowledged("withdraw")
        expected_total = sum(before.values()) + deposits - withdrawals
        actual_total = sum(after.values())

        checks = {
            "all_operations_resolved": not unresolved,
            "no_unexpected_errors": not unexpected,
            "ledger_matches_acknowledged_operations": not ledger_mismatch,
            "repeated_requests_got_identical_answers": not inconsistent_repeats,
            "money_conserved": actual_total == expected_total,
            "no_negative_balances": all(value >= 0 for value in after.values()),
            "no_duplicate_effects": not any(duplicates.values()),
            "ledger_chain_intact": not any(broken_chain.values()) and not any(stale_balance.values()),
            "2pc_outcomes_match_client_answers": not coordinator_mismatch,
            "2pc_all_terminal": not unterminated and not open_prepared,
            "2pc_atomic_on_participants": not atomicity_violations,
        }
        return {
            "checks": checks,
            "passed": all(checks.values()),
            "distinct_requests": len(unique),
            "ledger_rows_verified": sum(sum(rows.values()) for rows in expected_rows.values()),
            "total_before": str(sum(before.values())),
            "acknowledged_deposits": str(deposits),
            "acknowledged_withdrawals": str(withdrawals),
            "total_expected": str(expected_total),
            "total_actual": str(actual_total),
            "coordinator_states": dict(Counter(coordinator_states.values())),
            "participant_rows": sum(len(roles) for roles in participants.values()),
            "problems": {
                key: value for key, value in {
                    "unresolved": unresolved[:10],
                    "unexpected": dict(list(unexpected.items())[:10]),
                    "ledger_mismatch": dict(list(ledger_mismatch.items())[:10]),
                    "inconsistent_repeats": dict(list(inconsistent_repeats.items())[:5]),
                    "coordinator_mismatch": dict(list(coordinator_mismatch.items())[:10]),
                    "unterminated": unterminated,
                    "open_prepared": open_prepared[:10],
                    "duplicates": duplicates if any(duplicates.values()) else None,
                    "broken_chain": broken_chain if any(broken_chain.values()) else None,
                    "stale_balance": stale_balance if any(stale_balance.values()) else None,
                    "atomicity_violations": dict(list(atomicity_violations.items())[:10]),
                }.items() if value
            },
        }

    # -------------------------------------------------------------- framework

    def under_load(
        self,
        name: str,
        disruption: Callable[[str, list[LoadGenerator]], dict[str, Any]],
        concurrency: int | None = None,
        cross_shard_ratio: float = 0.5,
    ) -> dict[str, Any]:
        run_id = f"k8s-{self.session}-{name.replace(':', '-')}"
        before = self.balances()
        generator = LoadGenerator(
            self.gateway, run_id, concurrency or self.concurrency, self.settle_timeout,
            cross_shard_ratio=cross_shard_ratio, seed=abs(hash(run_id)) % 10_000,
        )
        generators = [generator]
        generator.start()
        details: dict[str, Any] = {}
        error = None
        try:
            time.sleep(5)
            details = disruption(run_id, generators)
            time.sleep(5)
        except Exception as exception:  # report and still verify the load
            error = f"{type(exception).__name__}: {exception}"
        finally:
            for item in generators:
                item.stop()
        invariants = self.verify(run_id, generators, before)
        passed = error is None and details.get("passed", True) and invariants["passed"]
        return {
            "passed": passed,
            **({"error": error} if error else {}),
            "details": details,
            "load": [item.summary() for item in generators],
            "invariants": invariants,
        }

    # -------------------------------------------------------------- scenarios

    def scenario_network_policy(self) -> dict[str, Any]:
        probe = (
            "import socket,sys\n"
            "for target in sys.argv[1:]:\n"
            "  host,port=target.rsplit(':',1); s=socket.socket(); s.settimeout(3)\n"
            "  try: s.connect((host,int(port))); print(target,'allowed')\n"
            "  except OSError: print(target,'blocked')\n"
            "  finally: s.close()\n"
        )
        expected = {
            "deploy/shard-a-app": {
                "shard-a-db-rw:5432": "allowed", "shard-b-db-rw:5432": "blocked",
                "coordinator-db-rw:5432": "blocked", "shard-b-app:8000": "blocked",
            },
            "deploy/shard-b-app": {
                "shard-b-db-rw:5432": "allowed", "shard-a-db-rw:5432": "blocked",
                "coordinator-db-rw:5432": "blocked", "shard-a-app:8000": "blocked",
            },
            "deploy/gateway": {
                "shard-a-app:8000": "allowed", "shard-b-app:8000": "allowed",
                "coordinator-db-rw:5432": "allowed", "shard-a-db-rw:5432": "blocked",
                "shard-b-db-rw:5432": "blocked",
            },
            "deploy/db-controller": {
                "shard-a-db-rw:5432": "allowed", "shard-a-app:8000": "blocked",
                "gateway:8000": "allowed",
            },
        }
        observed: dict[str, dict[str, str]] = {}
        for source, targets in expected.items():
            output = self.k8s.run("exec", source, "--", "python", "-c", probe, *targets)
            observed[source] = dict(line.split() for line in output.splitlines())
        return {"passed": observed == expected, "observed": observed}

    def scenario_baseline(self) -> dict[str, Any]:
        def disruption(_: str, __: list[LoadGenerator]) -> dict[str, Any]:
            time.sleep(self.duration)
            return {}
        return self.under_load("baseline", disruption)

    def scenario_pod_failures(self) -> dict[str, Any]:
        def disruption(_: str, __: list[LoadGenerator]) -> dict[str, Any]:
            killed = []
            deadline = time.monotonic() + self.duration
            index = 0
            while time.monotonic() < deadline:
                deployment = DEPLOYMENTS[index % len(DEPLOYMENTS)]
                index += 1
                ready = self.k8s.ready_pods(deployment)
                if ready:
                    victim = random.choice(ready)
                    self.k8s.run("delete", "pod", victim, "--grace-period=0", "--force", "--wait=false")
                    killed.append(victim)
                time.sleep(8)
            started = time.monotonic()
            self.k8s.wait_deployments(DEPLOYMENTS)
            survivors = {name: self.k8s.ready_pods(name) for name in DEPLOYMENTS}
            alive = [pod for pods in survivors.values() for pod in pods]
            return {
                "passed": not set(killed) & set(alive),
                "killed_pods": killed,
                "seconds_to_full_strength_after_last_kill": round(time.monotonic() - started, 1),
                "ready_pods_after": survivors,
            }
        return self.under_load("pod-failures", disruption)

    def scenario_db_pod_delete(self, cluster: str) -> dict[str, Any]:
        def disruption(_: str, __: list[LoadGenerator]) -> dict[str, Any]:
            leader_before = self.k8s.leader(cluster)
            pods_before = {pod["metadata"]["name"]: pod["metadata"]["uid"] for pod in self.k8s.pods(cluster)}
            started = time.monotonic()
            self.k8s.run("delete", "pod", "-l", f"app.kubernetes.io/name={cluster}", "--wait=false")
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                pods = self.k8s.pods(cluster)
                if (
                    len(pods) == len(pods_before)
                    and all(pod["metadata"]["uid"] not in pods_before.values() for pod in pods)
                    and all(self.k8s.is_ready(pod) for pod in pods)
                ):
                    break
                time.sleep(2)
            recreated = round(time.monotonic() - started, 1)
            replication = self.k8s.wait_replication(cluster)
            return {
                "passed": self.k8s.leader(cluster) == leader_before,
                "deleted_pods": sorted(pods_before),
                "seconds_until_pods_ready": recreated,
                "extra_seconds_until_sync_standby": replication,
                "leader_unchanged": leader_before[0],
            }
        return self.under_load(f"db-pod-delete:{cluster}", disruption)

    def scenario_db_switchover(self, cluster: str) -> dict[str, Any]:
        def disruption(_: str, __: list[LoadGenerator]) -> dict[str, Any]:
            leader_before, epoch_before = self.k8s.leader(cluster)
            started = time.monotonic()
            output = self.k8s.run(
                "exec", "deploy/db-controller", "--", "python", "db_controller.py", "failover", cluster,
                timeout=240,
            )
            promoted = round(time.monotonic() - started, 1)
            replication = self.k8s.wait_replication(cluster)
            leader_after, epoch_after = self.k8s.leader(cluster)
            return {
                "passed": leader_after != leader_before and epoch_after == epoch_before + 1,
                "leader_before": leader_before,
                "leader_after": leader_after,
                "epoch": [epoch_before, epoch_after],
                "seconds_to_promote": promoted,
                "extra_seconds_until_old_primary_is_sync_standby": replication,
                "controller_output": output.splitlines()[-3:],
            }
        return self.under_load(f"db-switchover:{cluster}", disruption)

    def scenario_db_failover(self, cluster: str) -> dict[str, Any]:
        app = APP_FOR_DATABASE[cluster]

        def disruption(_: str, __: list[LoadGenerator]) -> dict[str, Any]:
            leader_before, epoch_before = self.k8s.leader(cluster)
            app_restarts_before = self.k8s.restarts(app)
            node, container_id = self.k8s.container_task(leader_before, "postgres")
            self.k8s.freeze(node, container_id)
            frozen_at = time.monotonic()
            ready_app_pods: list[int] = []
            try:
                leader_after = leader_before
                while time.monotonic() - frozen_at < 120:
                    ready_app_pods.append(len(self.k8s.ready_pods(app)))
                    leader_after, _ = self.k8s.leader(cluster)
                    if leader_after != leader_before:
                        break
                    time.sleep(1)
                failover_seconds = round(time.monotonic() - frozen_at, 1)
                if leader_after == leader_before:
                    raise TimeoutError(f"{cluster}: no automatic failover within 120 s")
                writable_deadline = time.monotonic() + 60
                while time.monotonic() < writable_deadline:
                    ready_app_pods.append(len(self.k8s.ready_pods(app)))
                    if self.k8s.psql(cluster, "SELECT NOT pg_is_in_recovery()") == [["t"]]:
                        break
                    time.sleep(1)
                writable_seconds = round(time.monotonic() - frozen_at, 1)
            finally:
                self.k8s.thaw(node, container_id)
            replication = self.k8s.wait_replication(cluster)
            self.k8s.wait_deployments((app,))
            app_restarts_after = self.k8s.restarts(app)
            restarted = {
                pod: app_restarts_after[pod] - count
                for pod, count in app_restarts_before.items()
                if pod in app_restarts_after and app_restarts_after[pod] != count
            }
            _, epoch_after = self.k8s.leader(cluster)
            return {
                "passed": epoch_after == epoch_before + 1 and not restarted,
                "frozen_primary": leader_before,
                "new_primary": leader_after,
                "epoch": [epoch_before, epoch_after],
                "seconds_to_failover": failover_seconds,
                "seconds_until_new_primary_writable": writable_seconds,
                "extra_seconds_until_old_primary_rejoined_as_sync_standby": replication,
                f"min_ready_{app}_pods_during_outage": min(ready_app_pods, default=None),
                f"{app}_container_restarts_during_failover": restarted,
            }
        return self.under_load(f"db-failover:{cluster}", disruption)

    def scenario_rolling_update(self) -> dict[str, Any]:
        if not self.image:
            return {"passed": True, "skipped": "pass --image <name:tag> (built and loaded into minikube)"}

        def disruption(_: str, __: list[LoadGenerator]) -> dict[str, Any]:
            rollouts = {}
            for deployment in DEPLOYMENTS:
                pods_before = set(self.k8s.ready_pods(deployment))
                started = time.monotonic()
                self.k8s.run("set", "image", f"deployment/{deployment}", f"*={self.image}")
                self.k8s.run("rollout", "status", f"deployment/{deployment}", "--timeout=300s", timeout=320)
                pods_after = set(self.k8s.ready_pods(deployment))
                images = {
                    container["image"]
                    for pod in self.k8s.pods(deployment)
                    if not pod["metadata"].get("deletionTimestamp")
                    for container in pod["spec"]["containers"]
                }
                rollouts[deployment] = {
                    "seconds": round(time.monotonic() - started, 1),
                    "replaced_all_pods": not pods_before & pods_after,
                    "images": sorted(images),
                }
            return {
                "passed": all(
                    item["replaced_all_pods"] and item["images"] == [self.image]
                    for item in rollouts.values()
                ),
                "image": self.image,
                "rollouts": rollouts,
            }
        return self.under_load("rolling-update", disruption)

    def scenario_rollback(self) -> dict[str, Any]:
        deployment = "shard-a-app"

        def disruption(_: str, __: list[LoadGenerator]) -> dict[str, Any]:
            def revision() -> str:
                return self.k8s.json("get", "deployment", deployment)["metadata"]["annotations"][
                    "deployment.kubernetes.io/revision"
                ]
            good_revision = revision()
            good_pods = set(self.k8s.ready_pods(deployment))
            self.k8s.run("set", "env", f"deployment/{deployment}", "BANK_FAIL_READINESS=true")
            bad_revision = revision()
            ready_counts = []
            broken_pods: set[str] = set()
            observe_until = time.monotonic() + 30
            while time.monotonic() < observe_until:
                pods = self.k8s.pods(deployment)
                ready = {pod["metadata"]["name"] for pod in pods if self.k8s.is_ready(pod)}
                broken_pods |= {pod["metadata"]["name"] for pod in pods} - good_pods
                ready_counts.append(len(ready))
                time.sleep(2)
            broken_ever_ready = bool(broken_pods & set(self.k8s.ready_pods(deployment)))
            started = time.monotonic()
            self.k8s.run("rollout", "undo", f"deployment/{deployment}")
            self.k8s.run("rollout", "status", f"deployment/{deployment}", "--timeout=180s", timeout=200)
            env_names = {
                item["name"]
                for container in self.k8s.json("get", "deployment", deployment)["spec"]["template"]["spec"]["containers"]
                for item in container.get("env", [])
            }
            return {
                "passed": (
                    bool(broken_pods)
                    and not broken_ever_ready
                    and min(ready_counts) >= len(good_pods)
                    and "BANK_FAIL_READINESS" not in env_names
                ),
                "revisions": {"good": good_revision, "broken": bad_revision, "after_undo": revision()},
                "broken_pods_never_ready": sorted(broken_pods),
                "ready_pods_while_broken_release_rolled_out": min(ready_counts),
                "seconds_to_roll_back": round(time.monotonic() - started, 1),
            }
        return self.under_load("rollback", disruption)

    def scenario_autoscale(self) -> dict[str, Any]:
        def hpa_replicas() -> dict[str, int]:
            return {
                item["metadata"]["name"]: item.get("status", {}).get("currentReplicas", 0)
                for item in self.k8s.json("get", "hpa")["items"]
            }

        minimum = {
            item["metadata"]["name"]: item["spec"]["minReplicas"]
            for item in self.k8s.json("get", "hpa")["items"]
        }
        idle_since = time.monotonic()
        while hpa_replicas() != minimum:
            if time.monotonic() - idle_since > 420:
                raise TimeoutError(f"HPAs did not return to their minimum while idle: {hpa_replicas()}")
            time.sleep(10)

        def disruption(run_id: str, generators: list[LoadGenerator]) -> dict[str, Any]:
            burst = LoadGenerator(
                self.gateway, f"{run_id}-burst", self.concurrency * 6, self.settle_timeout,
                cross_shard_ratio=0.2, seed=7,
            )
            generators.append(burst)
            burst.start()
            timeline = []
            started = time.monotonic()
            scaled_at = None
            while time.monotonic() - started < 240:
                replicas = hpa_replicas()
                timeline.append({"t": round(time.monotonic() - started), **replicas})
                if scaled_at is None and any(replicas[name] > minimum[name] for name in replicas):
                    scaled_at = time.monotonic() - started
                if scaled_at is not None and time.monotonic() - started > scaled_at + 45:
                    break
                time.sleep(5)
            peak = {name: max(point[name] for point in timeline) for name in minimum}
            burst.stop()
            scale_down_started = time.monotonic()
            scaled_down = None
            while time.monotonic() - scale_down_started < 300:
                replicas = hpa_replicas()
                timeline.append({"t": round(time.monotonic() - started), **replicas})
                if replicas == minimum:
                    scaled_down = round(time.monotonic() - scale_down_started)
                    break
                time.sleep(10)
            return {
                "passed": scaled_at is not None,
                "min_replicas": minimum,
                "peak_replicas": peak,
                "seconds_until_scale_up": None if scaled_at is None else round(scaled_at),
                "seconds_until_back_to_min_after_burst": scaled_down,
                "timeline": timeline,
            }
        return self.under_load("autoscale", disruption, concurrency=2)

    def scenario_backup(self) -> dict[str, Any]:
        def disruption(run_id: str, _: list[LoadGenerator]) -> dict[str, Any]:
            job = f"backup-{run_id[-24:]}".lower()
            self.k8s.run("create", "job", job, "--from=cronjob/db-backup")
            self.k8s.run("wait", "--for=condition=complete", f"job/{job}", "--timeout=240s", timeout=260)
            logs = self.k8s.run("logs", f"job/{job}")
            return {"passed": "backup complete" in logs, "job": job, "log_tail": logs.splitlines()[-6:]}
        return self.under_load("backup", disruption)

    def scenario_concurrent_transactions(self) -> dict[str, Any]:
        """Overdraft race with an exactly known answer, plus colliding transfers."""

        def disruption(run_id: str, generators: list[LoadGenerator]) -> dict[str, Any]:
            race = LoadGenerator(self.gateway, f"{run_id}-race", 0, self.settle_timeout)
            generators.append(race)
            account = RESERVED_ACCOUNTS[0]
            balance = self.balances()[account]
            if balance < Decimal("10.00"):
                race.execute_new(f"{run_id}-fund", "deposit", account, Decimal("10.00"))
                balance = self.balances()[account]
            amount = (balance / 10).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            attempts = 20
            withdrawals = [
                Operation(f"{run_id}-withdraw-{index}", "withdraw", account, amount)
                for index in range(attempts)
            ]
            race.run_simultaneously(withdrawals)
            granted = sum(op.outcome == "success" for op in withdrawals)
            expected_granted = min(attempts, int(balance // amount))
            api_balance = self.balances()[account]
            database_balance = Decimal(self.k8s.psql(
                "shard-a-db", f"SELECT balance FROM accounts WHERE wallet = '{account}'")[0][0])

            pairs = [
                ("A0001", "A1001"), ("A1001", "A0001"),      # opposite cross-shard
                ("A0002", "A1002"), ("A1002", "A0002"),
                ("A0001", "A0002"), ("A0002", "A0001"),      # opposite same-shard
                ("A1001", "A1002"), ("A1002", "A1001"),
            ]
            collisions = [
                Operation(f"{run_id}-collide-{index}", "transfer", source, Decimal("0.07"), destination)
                for index, (source, destination) in enumerate(pairs * 5)
            ]
            race.run_simultaneously(collisions)
            return {
                "passed": (
                    granted == expected_granted
                    and api_balance == database_balance == balance - amount * granted
                ),
                "overdraft_race": {
                    "account": account,
                    "balance_before": str(balance),
                    "simultaneous_withdrawals": attempts,
                    "amount_each": str(amount),
                    "expected_granted": expected_granted,
                    "granted": granted,
                    "rejected_insufficient_funds": sum(op.outcome == "rejected" for op in withdrawals),
                    "balance_after_api": str(api_balance),
                    "balance_after_database": str(database_balance),
                },
                "colliding_transfers": {
                    "simultaneous": len(collisions),
                    "outcomes": dict(Counter(op.outcome for op in collisions)),
                },
            }
        return self.under_load("concurrent-transactions", disruption)

    def transfer_owner(self, transaction_id: str) -> str | None:
        """Gateway Pod holding the cluster-wide 2PC ownership lock of a transfer."""
        rows = self.k8s.psql("coordinator-db", (
            f"WITH k AS (SELECT hashtextextended('bank-2pc:{transaction_id}', 0) AS v) "
            "SELECT host(a.client_addr) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid, k "
            "WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1 "
            "AND l.classid::bigint = ((k.v >> 32) & 4294967295) "
            "AND l.objid::bigint = (k.v & 4294967295)"
        ))
        if not rows:
            return None
        for pod in self.k8s.pods("gateway"):
            if pod.get("status", {}).get("podIP") == rows[0][0]:
                return pod["metadata"]["name"]
        return None

    def transfer_state(self, transaction_id: str) -> dict[str, Any]:
        rows = self.k8s.psql("coordinator-db", (
            "SELECT state, coalesce(source_vote::text, ''), coalesce(destination_vote::text, ''), "
            "source_ack, destination_ack FROM bank_coordinated_transfers "
            f"WHERE transaction_id = '{transaction_id}'"
        ))
        participants = {}
        ledger = {}
        for cluster in ("shard-a-db", "shard-b-db"):
            for role, state in self.k8s.psql(cluster, (
                "SELECT role, state FROM bank_prepared_transfers "
                f"WHERE transaction_id = '{transaction_id}'"
            )):
                participants[role] = state
            ledger[cluster] = int(self.k8s.psql(cluster, (
                f"SELECT count(*) FROM bank_transactions WHERE request_id = '{transaction_id}'"
            ))[0][0])
        if not rows:
            return {"coordinator": None, "participants": participants, "ledger_rows": ledger}
        state, source_vote, destination_vote, source_ack, destination_ack = rows[0]
        return {
            "coordinator": state,
            "source_vote": source_vote or None,
            "destination_vote": destination_vote or None,
            "acks": [source_ack == "t", destination_ack == "t"],
            "participants": participants,
            "ledger_rows": ledger,
        }

    def scenario_coordinator_interruption(self) -> dict[str, Any]:
        """Kill the owning gateway mid-2PC (after PREPARE, after COMMIT)."""
        pause = 30

        def interrupt(run_id: str, generator: LoadGenerator, case: str) -> dict[str, Any]:
            source, destination = RESERVED_ACCOUNTS
            transaction_id = f"{run_id}-{case}"
            balances_before = {account: self.balances()[account] for account in RESERVED_ACCOUNTS}
            wanted = (
                (lambda s: s["coordinator"] == "PREPARING" and s["source_vote"] == "true")
                if case == "after-prepare"
                else (lambda s: s["coordinator"] == "COMMIT")
            )
            def send_once() -> None:
                # A single attempt: the client does not retry while the
                # transfer is being recovered, so recovery is what finishes it.
                try:
                    httpx.post(
                        self.gateway + "/transactions",
                        json={"action": "transfer", "account": source,
                              "destination": destination, "amount": "1.00"},
                        headers={"X-Request-ID": transaction_id},
                        timeout=pause * 3,
                    )
                except httpx.HTTPError:
                    pass

            threading.Thread(target=send_once, daemon=True).start()
            deadline = time.monotonic() + pause * 2 + 15
            frozen = self.transfer_state(transaction_id)
            while not (frozen["coordinator"] and wanted(frozen)):
                if time.monotonic() > deadline:
                    raise TimeoutError(f"{case}: transfer never reached the failpoint: {frozen}")
                time.sleep(0.5)
                frozen = self.transfer_state(transaction_id)
            owner = self.transfer_owner(transaction_id)
            if owner is None:
                raise RuntimeError(f"{case}: no gateway holds the ownership lock")
            self.k8s.run("delete", "pod", owner, "--grace-period=0", "--force", "--wait=false")
            killed_at = time.monotonic()
            recovered = self.transfer_state(transaction_id)
            while recovered["coordinator"] not in {"COMMITTED", "ABORTED"}:
                if time.monotonic() - killed_at > 120:
                    raise TimeoutError(f"{case}: not recovered within 120 s: {recovered}")
                time.sleep(1)
                recovered = self.transfer_state(transaction_id)
            recovery_seconds = round(time.monotonic() - killed_at, 1)
            retry = generator.execute_new(
                transaction_id, "transfer", source, Decimal("1.00"), destination)
            balances_after = {account: self.balances()[account] for account in RESERVED_ACCOUNTS}
            moved = balances_before[source] - balances_after[source]
            if case == "after-prepare":
                correct = (
                    recovered["coordinator"] == "ABORTED"
                    and "committed" not in recovered["participants"].values()
                    and not any(recovered["ledger_rows"].values())
                    and retry.outcome == "aborted"
                    and moved == 0
                )
            else:
                correct = (
                    recovered["coordinator"] == "COMMITTED"
                    and recovered["participants"] == {"debit": "committed", "credit": "committed"}
                    and recovered["ledger_rows"] == {"shard-a-db": 1, "shard-b-db": 1}
                    and retry.outcome == "success"
                    and moved == Decimal("1.00")
                    and balances_after[destination] - balances_before[destination] == Decimal("1.00")
                )
            return {
                "passed": correct,
                "transaction_id": transaction_id,
                "killed_owner": owner,
                "state_when_killed": frozen,
                "seconds_until_recovered_by_another_gateway": recovery_seconds,
                "state_after_recovery": recovered,
                "client_retry_with_same_id": retry.outcome,
                "money_moved": str(moved),
            }

        def disruption(run_id: str, generators: list[LoadGenerator]) -> dict[str, Any]:
            probe = LoadGenerator(self.gateway, f"{run_id}-probe", 0, self.settle_timeout)
            generators.append(probe)
            if self.balances()[RESERVED_ACCOUNTS[0]] < Decimal("5.00"):
                probe.execute_new(f"{run_id}-fund", "deposit", RESERVED_ACCOUNTS[0], Decimal("10.00"))
            self.k8s.run(
                "set", "env", "deployment/gateway",
                f"BANK_EXPERIMENT_PAUSE_AFTER_PREPARE_SECONDS={pause}",
                f"BANK_EXPERIMENT_PAUSE_AFTER_COMMIT_SECONDS={pause}",
            )
            try:
                self.k8s.run("rollout", "status", "deployment/gateway", "--timeout=300s", timeout=320)
                cases = {case: interrupt(run_id, probe, case) for case in ("after-prepare", "after-commit")}
            finally:
                self.k8s.run(
                    "set", "env", "deployment/gateway",
                    "BANK_EXPERIMENT_PAUSE_AFTER_PREPARE_SECONDS-",
                    "BANK_EXPERIMENT_PAUSE_AFTER_COMMIT_SECONDS-",
                )
                self.k8s.run("rollout", "status", "deployment/gateway", "--timeout=300s", timeout=320)
            return {"passed": all(case["passed"] for case in cases.values()), **cases}

        # Background load stays on single-shard operations: every cross-shard
        # transfer would also stop at the failpoints.
        return self.under_load("coordinator-interruption", disruption, cross_shard_ratio=0.0)

    def scenario_repeated_requests(self) -> dict[str, Any]:
        """Lost responses, concurrent duplicates and late replays of one request."""

        def disruption(run_id: str, generators: list[LoadGenerator]) -> dict[str, Any]:
            repeats = LoadGenerator(self.gateway, f"{run_id}-repeat", 0, self.settle_timeout)
            generators.append(repeats)
            cases = {
                "deposit": ("deposit", "A0003", Decimal("0.11"), None),
                "same-shard-transfer": ("transfer", "A0002", Decimal("0.12"), "A0004"),
                "cross-shard-transfer": ("transfer", "A1004", Decimal("0.13"), "A0003"),
            }
            report = {}
            for case, (kind, account, amount, destination) in cases.items():
                request_id = f"{run_id}-{case}"
                body = Operation(request_id, kind, account, amount, destination).body()
                try:
                    httpx.post(self.gateway + "/transactions", json=body,
                               headers={"X-Request-ID": request_id}, timeout=0.02)
                    first_attempt = "answered"
                except httpx.HTTPError:
                    first_attempt = "response lost (client timed out)"
                duplicates = [
                    Operation(request_id, kind, account, amount, destination) for _ in range(16)
                ]
                repeats.run_simultaneously(duplicates)
                replays = [repeats.execute_new(request_id, kind, account, amount, destination) for _ in range(5)]
                conflicting = httpx.post(
                    self.gateway + "/transactions",
                    json={**body, "amount": str(amount + 1)},
                    headers={"X-Request-ID": request_id},
                    timeout=30,
                )
                answers = {json.dumps(op.response_body, sort_keys=True) for op in duplicates + replays}
                report[case] = {
                    "request_id": request_id,
                    "first_attempt": first_attempt,
                    "submissions": 1 + len(duplicates) + len(replays),
                    "gateways_that_answered": sorted({op.gateway_replica for op in duplicates + replays if op.gateway_replica}),
                    "distinct_answers": len(answers),
                    "outcome": duplicates[0].outcome,
                    "conflicting_reuse": f"{conflicting.status_code} {error_code(conflicting)}",
                }
            for case in report.values():
                request_id = case["request_id"]
                case["ledger_rows"] = {
                    cluster: int(self.k8s.psql(cluster, (
                        f"SELECT count(*) FROM bank_transactions WHERE request_id = '{request_id}'"
                    ))[0][0])
                    for cluster in ("shard-a-db", "shard-b-db")
                }
                case["idempotency_records"] = {
                    cluster: int(self.k8s.psql(cluster, (
                        f"SELECT count(*) FROM bank_operations WHERE request_id = '{request_id}'"
                    ))[0][0])
                    for cluster in ("shard-a-db", "shard-b-db")
                }
                case["idempotency_records"]["coordinator-db"] = int(self.k8s.psql("coordinator-db", (
                    "SELECT count(*) FROM bank_coordinated_transfers "
                    f"WHERE transaction_id = '{request_id}'"
                ))[0][0])
            expected = {
                "deposit": ({"shard-a-db": 1, "shard-b-db": 0},
                            {"shard-a-db": 1, "shard-b-db": 0, "coordinator-db": 0}),
                "same-shard-transfer": ({"shard-a-db": 2, "shard-b-db": 0},
                                        {"shard-a-db": 1, "shard-b-db": 0, "coordinator-db": 0}),
                "cross-shard-transfer": ({"shard-a-db": 1, "shard-b-db": 1},
                                         {"shard-a-db": 1, "shard-b-db": 1, "coordinator-db": 1}),
            }
            return {
                "passed": all(
                    case["outcome"] == "success"
                    and case["distinct_answers"] == 1
                    and (case["ledger_rows"], case["idempotency_records"]) == expected[name]
                    and case["conflicting_reuse"].endswith("request_id_conflict")
                    for name, case in report.items()
                ),
                **report,
            }
        return self.under_load("repeated-requests", disruption)

    def run(self, names: list[str]) -> dict[str, Any]:
        self.fund_accounts()
        results = {}
        for name in names:
            print(f"[{datetime.now():%H:%M:%S}] running {name} ...", flush=True)
            kind, _, argument = name.partition(":")
            method = getattr(self, "scenario_" + kind.replace("-", "_"))
            results[name] = method(argument) if argument else method()
            print(
                f"[{datetime.now():%H:%M:%S}] {name}: "
                f"{'PASSED' if results[name].get('passed') else 'FAILED'}",
                flush=True,
            )
        return {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "gateway": self.gateway,
            "session": self.session,
            "passed": all(result.get("passed") for result in results.values()),
            "results": results,
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--gateway", required=True, help="Public gateway URL, e.g. from `minikube service gateway --url`.")
    parser.add_argument("--namespace", default="banking")
    parser.add_argument("--profile", default="banking", help="Minikube profile.")
    parser.add_argument("--context", help="kubectl context (default: the current one).")
    parser.add_argument(
        "--scenarios",
        default="required",
        help=f"'required', 'all' or a comma-separated list of: {', '.join(PRESETS['all'])}",
    )
    parser.add_argument("--duration", type=float, default=40, help="Seconds of disruption for timed scenarios.")
    parser.add_argument("--concurrency", type=int, default=8, help="Concurrent clients for background load.")
    parser.add_argument("--settle-timeout", type=float, default=180, help="Seconds clients keep retrying after load stops.")
    parser.add_argument("--image", help="New image for the rolling-update scenario, e.g. mini-banking-app:1.0.1.")
    parser.add_argument("--output", type=Path, help="Write the JSON report to this file.")
    args = parser.parse_args(argv)

    names = list(PRESETS.get(args.scenarios, args.scenarios.split(",")))
    experiments = Experiments(args)
    try:
        report = experiments.run(names)
    finally:
        experiments.close()
    rendered = json.dumps(report, indent=2, default=str)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
