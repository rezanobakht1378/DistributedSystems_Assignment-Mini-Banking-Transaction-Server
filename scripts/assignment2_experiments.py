from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx


ROOT = Path(__file__).resolve().parents[1]
EXPERIMENTS = (
    "shard-distribution",
    "replica-load",
    "concurrent-replica",
    "replica-interruption",
    "transfers",
    "interrupted-2pc",
    "concurrent-2pc",
)
AMOUNT = "0.01"


class Assignment2Experiments:
    def __init__(
        self,
        gateway_url: str,
        requests: int,
        project_name: str | None,
    ) -> None:
        self.gateway_url = gateway_url.rstrip("/")
        self.requests = requests
        self.project_name = project_name
        self.client = httpx.Client(timeout=10)

    def close(self) -> None:
        self.client.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        body: dict[str, str] | None = None,
        request_id: str | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        headers = {"X-Request-ID": request_id} if request_id else None
        response = self.client.request(
            method,
            self.gateway_url + path,
            params=params,
            json=body,
            headers=headers,
            timeout=timeout,
        )
        response.raise_for_status()
        return response

    def balance(self, account: str) -> Decimal:
        response = self.request(
            "GET",
            "/balance/inquiry",
            params={"account": account},
        )
        return Decimal(str(response.json()["balance"]))

    def compose(self, *arguments: str, check: bool = True) -> str:
        command = ["docker", "compose"]
        if self.project_name:
            command.extend(("-p", self.project_name))
        command.extend(arguments)
        result = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if check and result.returncode:
            raise RuntimeError(
                f"docker compose {' '.join(arguments)} failed: "
                f"{result.stderr.strip() or result.stdout.strip()}"
            )
        return result.stdout.strip()

    def transfer(
        self,
        source: str,
        destination: str,
        *,
        amount: str = AMOUNT,
        request_id: str | None = None,
        timeout: float | None = None,
    ) -> tuple[httpx.Response, str]:
        identifier = request_id or uuid.uuid4().hex
        response = self.client.post(
            self.gateway_url + "/transactions",
            json={
                "action": "transfer",
                "account": source,
                "destination": destination,
                "amount": amount,
            },
            headers={"X-Request-ID": identifier},
            timeout=timeout or 30,
        )
        return response, identifier

    def experiment_shard_distribution(self) -> dict[str, Any]:
        expected = {
            "shard-a-db": ("banking_shard_a", "A0001", ("A0001",)),
            "shard-b-db": ("banking_shard_b", "A1001", ("A1001",)),
        }
        database_evidence: dict[str, list[str]] = {}
        for service, (database, _, _) in expected.items():
            query = (
                "SELECT wallet FROM accounts "
                "WHERE wallet IN ('A0001', 'A1001') ORDER BY wallet;"
            )
            output = self.compose(
                "exec",
                "-T",
                service,
                "psql",
                "-U",
                "banking",
                "-d",
                database,
                "-At",
                "-c",
                query,
            )
            database_evidence[service] = [
                line for line in output.splitlines() if line
            ]
        if database_evidence != {
            "shard-a-db": ["A0001"],
            "shard-b-db": ["A1001"],
        }:
            raise AssertionError(f"Unexpected account placement: {database_evidence}")

        routed = {
            account: self.request(
                "GET",
                "/balance/inquiry",
                params={"account": account},
            ).headers["X-Backend-Replica"]
            for account in ("A0001", "A1001")
        }
        return {
            "databases": database_evidence,
            "gateway_requests": routed,
            "passed": True,
        }

    def experiment_replica_load(self) -> dict[str, Any]:
        replicas: dict[str, int] = {}
        for _ in range(self.requests):
            response = self.request(
                "GET",
                "/balance/inquiry",
                params={"account": "A0001"},
            )
            replica = response.headers["X-Backend-Replica"]
            replicas[replica] = replicas.get(replica, 0) + 1
        if len(replicas) < 2:
            raise AssertionError(
                f"Requests did not reach both Shard A replicas: {replicas}"
            )
        return {"requests": self.requests, "replica_counts": replicas, "passed": True}

    def experiment_concurrent_replica_operations(self) -> dict[str, Any]:
        account = "A0001"
        amount = Decimal(AMOUNT)
        before = self.balance(account)

        def deposit(index: int) -> httpx.Response:
            return self.request(
                "POST",
                "/transactions",
                body={
                    "action": "deposit",
                    "account": account,
                    "amount": AMOUNT,
                },
                request_id=f"experiment-deposit-{uuid.uuid4().hex}-{index}",
            )

        with ThreadPoolExecutor(max_workers=min(self.requests, 32)) as pool:
            responses = list(pool.map(deposit, range(self.requests)))
        after = self.balance(account)
        replicas = sorted(
            {response.headers["X-Backend-Replica"] for response in responses}
        )
        expected = before + amount * self.requests
        if after != expected or len(replicas) < 2:
            raise AssertionError(
                f"Concurrent deposits mismatch: before={before}, "
                f"after={after}, expected={expected}, replicas={replicas}"
            )
        return {
            "account": account,
            "concurrent_deposits": self.requests,
            "amount_each": AMOUNT,
            "before": str(before),
            "after": str(after),
            "replicas": replicas,
            "passed": True,
        }

    def experiment_replica_interruption(self) -> dict[str, Any]:
        service = "shard-a-app-1"
        self.compose("stop", service)
        try:
            replicas = []
            for _ in range(5):
                response = self.request(
                    "GET",
                    "/balance/inquiry",
                    params={"account": "A0001"},
                )
                replicas.append(response.headers["X-Backend-Replica"])
            if not replicas or any("shard-a-app-1" in name for name in replicas):
                raise AssertionError(
                    f"Gateway routed traffic to the stopped replica: {replicas}"
                )
        finally:
            self.compose("start", service)
        return {
            "stopped_service": service,
            "successful_requests": len(replicas),
            "selected_replicas": replicas,
            "replica_restarted": True,
            "passed": True,
        }

    def experiment_transfers(self) -> dict[str, Any]:
        local_before = (self.balance("A0001"), self.balance("A0002"))
        local_response, local_id = self.transfer("A0001", "A0002")
        local_response.raise_for_status()
        local_after = (self.balance("A0001"), self.balance("A0002"))
        local_ok = (
            local_before[0] - local_after[0] == Decimal(AMOUNT)
            and local_after[1] - local_before[1] == Decimal(AMOUNT)
            and sum(local_before) == sum(local_after)
        )

        cross_before = (self.balance("A0001"), self.balance("A1001"))
        cross_response, cross_id = self.transfer("A0001", "A1001")
        cross_response.raise_for_status()
        cross_after = (self.balance("A0001"), self.balance("A1001"))
        cross_state = self.request(
            "GET",
            f"/transactions/{cross_id}",
        ).json()["state"]
        cross_ok = (
            cross_before[0] - cross_after[0] == Decimal(AMOUNT)
            and cross_after[1] - cross_before[1] == Decimal(AMOUNT)
            and sum(cross_before) == sum(cross_after)
            and cross_state == "COMMITTED"
        )
        if not local_ok or not cross_ok:
            raise AssertionError(
                f"Transfer validation failed: local={local_ok}, "
                f"cross={cross_ok}, cross_state={cross_state}"
            )
        return {
            "same_shard": {"request_id": local_id, "total_preserved": local_ok},
            "cross_shard": {
                "request_id": cross_id,
                "state": cross_state,
                "total_preserved": cross_ok,
            },
            "passed": True,
        }

    def experiment_interrupted_2pc(self) -> dict[str, Any]:
        output = self.compose(
            "exec",
            "-T",
            "gateway",
            "python",
            "-c",
            "import os; print(os.getenv('BANK_EXPERIMENT_PAUSE_AFTER_COMMIT_SECONDS', '0'))",
        )
        try:
            pause_seconds = float(output)
        except ValueError as error:
            raise RuntimeError(
                f"Invalid Coordinator experiment failpoint value: {output!r}"
            ) from error
        if pause_seconds < 10:
            return {
                "skipped": True,
                "reason": (
                    "Set BANK_EXPERIMENT_PAUSE_AFTER_COMMIT_SECONDS to at least "
                    "10 and recreate the gateway before running this experiment."
                ),
            }

        identifier = f"experiment-recovery-{uuid.uuid4().hex}"
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(
            self.transfer,
            "A0001",
            "A1001",
            request_id=identifier,
            timeout=pause_seconds + 20,
        )
        gateway_stopped = False
        try:
            deadline = time.monotonic() + min(pause_seconds - 2, 30)
            observed = None
            while time.monotonic() < deadline:
                status_response = self.client.get(
                    self.gateway_url + f"/transactions/{identifier}"
                )
                if status_response.is_success:
                    observed = status_response.json()
                    if (
                        observed["state"] == "COMMIT"
                        and not observed["source_ack"]
                        and not observed["destination_ack"]
                    ):
                        break
                time.sleep(0.2)
            else:
                raise AssertionError(
                    f"Did not observe durable COMMIT before acknowledgements: {observed}"
                )

            self.compose("stop", "--timeout", "0", "gateway")
            gateway_stopped = True
            self.compose("start", "gateway")
            gateway_stopped = False
            self._wait_gateway()
            deadline = time.monotonic() + 45
            recovered = None
            while time.monotonic() < deadline:
                status_response = self.client.get(
                    self.gateway_url + f"/transactions/{identifier}"
                )
                if status_response.is_success:
                    recovered = status_response.json()
                    if recovered["state"] == "COMMITTED":
                        break
                time.sleep(0.5)
            if recovered is None or recovered["state"] != "COMMITTED":
                raise AssertionError(
                    f"Coordinator did not recover the durable COMMIT: {recovered}"
                )
            return {
                "transaction_id": identifier,
                "state_before_restart": observed["state"],
                "state_after_restart": recovered["state"],
                "source_ack": recovered["source_ack"],
                "destination_ack": recovered["destination_ack"],
                "passed": True,
            }
        finally:
            if gateway_stopped:
                self.compose("start", "gateway")
            if not future.done():
                future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)

    def _wait_gateway(self) -> None:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                response = self.client.get(self.gateway_url + "/health")
                if response.is_success:
                    return
            except httpx.RequestError:
                pass
            time.sleep(1)
        raise TimeoutError("Gateway did not become healthy after restart.")

    def experiment_concurrent_2pc(self) -> dict[str, Any]:
        pairs = (
            ("A0001", "A1001"),
            ("A0001", "A1002"),
            ("A0002", "A1001"),
        )
        accounts = ("A0001", "A0002", "A1001", "A1002")
        before = {account: self.balance(account) for account in accounts}

        def send_transfer(index: int) -> tuple[str, str]:
            source, destination = pairs[index % len(pairs)]
            response, identifier = self.transfer(source, destination)
            response.raise_for_status()
            return identifier, source

        count = max(6, self.requests)
        with ThreadPoolExecutor(max_workers=min(count, 32)) as pool:
            transactions = list(pool.map(send_transfer, range(count)))
        states = {}
        for identifier, _ in transactions:
            state = self.request(
                "GET",
                f"/transactions/{identifier}",
            ).json()["state"]
            states[state] = states.get(state, 0) + 1
        after = {account: self.balance(account) for account in accounts}
        conserved = sum(before.values()) == sum(after.values())
        if (
            states != {"COMMITTED": count}
            or not conserved
            or any(after[account] < 0 for account in accounts)
        ):
            raise AssertionError(
                f"Concurrent 2PC validation failed: states={states}, "
                f"before={before}, after={after}"
            )
        return {
            "transactions": count,
            "states": states,
            "before": {key: str(value) for key, value in before.items()},
            "after": {key: str(value) for key, value in after.items()},
            "total_conserved": conserved,
            "passed": True,
        }

    def run(self, names: list[str]) -> dict[str, Any]:
        methods = {
            "shard-distribution": self.experiment_shard_distribution,
            "replica-load": self.experiment_replica_load,
            "concurrent-replica": self.experiment_concurrent_replica_operations,
            "replica-interruption": self.experiment_replica_interruption,
            "transfers": self.experiment_transfers,
            "interrupted-2pc": self.experiment_interrupted_2pc,
            "concurrent-2pc": self.experiment_concurrent_2pc,
        }
        results = {}
        for name in names:
            print(f"Running {name}...", flush=True)
            results[name] = methods[name]()
        return {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "gateway": self.gateway_url,
            "compose_project": self.project_name,
            "results": results,
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the seven required Assignment 2 distributed-platform experiments."
    )
    parser.add_argument(
        "--gateway",
        default="http://localhost:18080",
        help="Public Gateway URL (default: http://localhost:18080).",
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=20,
        help="Request count for load/concurrency experiments (default: 20).",
    )
    parser.add_argument(
        "--project-name",
        help="Docker Compose project name when testing a non-default stack.",
    )
    parser.add_argument(
        "--experiments",
        default="all",
        help="Comma-separated experiment names, or 'all'.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to save the JSON results report.",
    )
    args = parser.parse_args(argv)
    parsed_gateway = urlsplit(args.gateway)
    if parsed_gateway.scheme not in {"http", "https"} or not parsed_gateway.netloc:
        parser.error("--gateway must be an absolute HTTP or HTTPS URL.")
    if args.requests < 2:
        parser.error("--requests must be at least 2.")
    selected = (
        list(EXPERIMENTS)
        if args.experiments == "all"
        else args.experiments.split(",")
    )
    unknown = sorted(set(selected) - set(EXPERIMENTS))
    if unknown:
        parser.error(f"Unknown experiment(s): {', '.join(unknown)}")
    runner = Assignment2Experiments(
        args.gateway,
        args.requests,
        args.project_name,
    )
    try:
        report = runner.run(selected)
    except (AssertionError, httpx.HTTPError, RuntimeError, TimeoutError) as error:
        print(f"Experiment failed: {error}", file=sys.stderr)
        return 1
    finally:
        runner.close()

    rendered = json.dumps(report, indent=2)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
        print(f"Results saved to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
