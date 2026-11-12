import time
import unittest

import httpx
from fastapi.testclient import TestClient

from gateway import create_app


class MemoryCoordinator:
    def __init__(self) -> None:
        self.transaction: dict[str, object] | None = None
        self.events: list[str] = []
        self.locked_elsewhere: set[str] = set()

    def initialize(self) -> None:
        pass

    def status(self) -> dict[str, bool]:
        return {"reachable": True, "writable": True}

    def state_counts(self) -> dict[str, int]:
        if self.transaction is None:
            return {}
        return {str(self.transaction["state"]): 1}

    def acquire_transaction_lock(
        self,
        transaction_id: str,
        wait_seconds: float,
    ) -> str | None:
        if transaction_id in self.locked_elsewhere:
            return None
        self.events.append(f"lock:{transaction_id}")
        return transaction_id

    def release_transaction_lock(self, handle: str) -> None:
        self.events.append(f"unlock:{handle}")

    def ensure_replica(self, shard_id: str, replica_id: str, url: str) -> None:
        pass

    def replica_urls(self, shard_id: str) -> list[str]:
        return []

    def begin(
        self,
        transaction_id: str,
        fingerprint: str,
        source_account: str,
        destination_account: str,
        amount,
    ) -> dict[str, object]:
        if self.transaction is None:
            self.transaction = {
                "transaction_id": transaction_id,
                "fingerprint": fingerprint,
                "source_account": source_account,
                "destination_account": destination_account,
                "amount": str(amount),
                "state": "PREPARING",
                "source_vote": None,
                "destination_vote": None,
                "source_ack": False,
                "destination_ack": False,
                "source_result": None,
                "destination_result": None,
                "response": None,
                "error_message": None,
            }
        return self.transaction.copy()

    def get(self, transaction_id: str) -> dict[str, object] | None:
        if (
            self.transaction is None
            or self.transaction["transaction_id"] != transaction_id
        ):
            return None
        return self.transaction.copy()

    def record_vote(
        self,
        transaction_id: str,
        participant: str,
        vote: bool,
        error_message: str | None = None,
    ) -> dict[str, object]:
        assert self.transaction is not None
        self.transaction[f"{participant}_vote"] = vote
        self.events.append(f"vote:{participant}:{vote}")
        return self.transaction.copy()

    def decide(self, transaction_id: str, decision: str) -> dict[str, object]:
        assert self.transaction is not None
        self.transaction["state"] = decision
        self.events.append(f"durable:{decision}")
        return self.transaction.copy()

    def record_ack(
        self,
        transaction_id: str,
        participant: str,
        result: dict[str, object] | None = None,
    ) -> dict[str, object]:
        assert self.transaction is not None
        self.transaction[f"{participant}_ack"] = True
        self.transaction[f"{participant}_result"] = result
        self.events.append(f"ack:{participant}")
        if (
            self.transaction["state"] == "ABORT"
            and self.transaction["source_ack"]
            and self.transaction["destination_ack"]
        ):
            self.transaction["state"] = "ABORTED"
        elif self.transaction["source_ack"] and self.transaction["destination_ack"]:
            self.transaction["state"] = "COMMITTED"
            source_result = self.transaction["source_result"]
            destination_result = self.transaction["destination_result"]
            assert isinstance(source_result, dict)
            assert isinstance(destination_result, dict)
            response = {
                "status": "success",
                "transaction_id": transaction_id,
                "source_account": self.transaction["source_account"],
                "source_balance": source_result["balance"],
                "destination": self.transaction["destination_account"],
                "destination_balance": destination_result["balance"],
            }
            self.transaction["response"] = response
        return self.transaction.copy()

    def pending(self) -> list[dict[str, object]]:
        if self.transaction is None or self.transaction["state"] not in {
            "PREPARING",
            "COMMIT",
            "ABORT",
        }:
            return []
        return [self.transaction.copy()]


class ShardGatewayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.calls: list[tuple[str, str]] = []

        def handle(request: httpx.Request) -> httpx.Response:
            replica = request.url.host
            self.calls.append((replica, request.url.path))
            if request.url.path == "/health":
                return httpx.Response(200)
            return httpx.Response(
                200,
                json={
                    "status": "success",
                    "replica": replica,
                    "path": request.url.path,
                },
            )

        self.app = create_app(
            {
                "shard-a": ["http://a1:8000", "http://a2:8000"],
                "shard-b": ["http://b1:8000", "http://b2:8000"],
            },
            transport=httpx.MockTransport(handle),
        )

    def test_gateway_routes_by_shard_and_round_robins_replicas(self) -> None:
        with TestClient(self.app) as client:
            first = client.get("/balance/inquiry?account=A0001")
            second = client.get("/balance/inquiry?account=A0001")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.json()["replica"], "a1")
        self.assertEqual(second.json()["replica"], "a2")
        self.assertEqual(first.headers["x-backend-replica"], "a1")
        self.assertEqual(second.headers["x-backend-replica"], "a2")

    def test_gateway_skips_unhealthy_replica(self) -> None:
        calls: list[str] = []

        def handle(request: httpx.Request) -> httpx.Response:
            replica = request.url.host
            calls.append(f"{replica}{request.url.path}")
            if request.url.path == "/health":
                return httpx.Response(503 if replica == "a1" else 200)
            return httpx.Response(200, json={"replica": replica})

        app = create_app(
            {
                "shard-a": ["http://a1:8000", "http://a2:8000"],
                "shard-b": ["http://b1:8000", "http://b2:8000"],
            },
            transport=httpx.MockTransport(handle),
        )
        with TestClient(app) as client:
            response = client.get("/balance/inquiry?account=A0001")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["replica"], "a2")
        self.assertNotIn("a1/balance/inquiry", calls)

    def test_gateway_fails_over_after_replica_connection_error(self) -> None:
        request_ids: list[str] = []

        def handle(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/health":
                return httpx.Response(200)
            request_ids.append(request.headers["x-request-id"])
            if request.url.host == "a1":
                raise httpx.ConnectError("replica disconnected", request=request)
            return httpx.Response(200, json={"replica": request.url.host})

        app = create_app(
            {
                "shard-a": ["http://a1:8000", "http://a2:8000"],
                "shard-b": ["http://b1:8000", "http://b2:8000"],
            },
            transport=httpx.MockTransport(handle),
        )
        with TestClient(app) as client:
            response = client.post(
                "/transactions",
                json={
                    "action": "deposit",
                    "account": "A0001",
                    "amount": "5",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["replica"], "a2")
        self.assertEqual(len(request_ids), 2)
        self.assertEqual(request_ids[0], request_ids[1])

    def test_gateway_does_not_resend_operation_after_upstream_timeout(self) -> None:
        calls: list[str] = []

        def handle(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/health":
                return httpx.Response(200)
            calls.append(request.url.host)
            raise httpx.ReadTimeout("replica is slow", request=request)

        app = create_app(
            {"shard-a": ["http://a1:8000", "http://a2:8000"], "shard-b": ["http://b1:8000"]},
            transport=httpx.MockTransport(handle),
        )
        with TestClient(app) as client:
            response = client.post(
                "/transactions",
                headers={"X-Request-ID": "slow-deposit"},
                json={"action": "deposit", "account": "A0001", "amount": "5"},
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"]["error"], "upstream_timeout")
        self.assertEqual(len(calls), 1)

    def test_gateway_tries_next_replica_when_one_is_busy(self) -> None:
        def handle(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/health":
                return httpx.Response(200)
            if request.url.host == "a1":
                return httpx.Response(503, json={"status": "error", "error": "server_busy"})
            return httpx.Response(200, json={"replica": request.url.host})

        app = create_app(
            {"shard-a": ["http://a1:8000", "http://a2:8000"], "shard-b": ["http://b1:8000"]},
            transport=httpx.MockTransport(handle),
        )
        with TestClient(app) as client:
            responses = [
                client.get("/balance/inquiry?account=A0001") for _ in range(2)
            ]

        self.assertEqual([item.status_code for item in responses], [200, 200])
        self.assertEqual({item.json()["replica"] for item in responses}, {"a2"})

    def test_gateway_routes_shard_b_accounts_to_shard_b_replicas(self) -> None:
        with TestClient(self.app) as client:
            response = client.get("/balance/inquiry?account=A1001")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["replica"].startswith("b"))

    def test_gateway_requires_coordinator_for_cross_shard_transfers(self) -> None:
        with TestClient(self.app) as client:
            response = client.post(
                "/transactions",
                json={
                    "action": "transfer",
                    "account": "A0001",
                    "destination": "A1001",
                    "amount": "5",
                },
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json()["detail"]["error"],
            "coordinator_unavailable",
        )
        self.assertEqual(self.calls, [])

    def test_transaction_status_returns_pending_for_unresolved_transaction(self) -> None:
        coordinator = MemoryCoordinator()
        coordinator.transaction = {
            "transaction_id": "pending-transfer",
            "state": "COMMIT",
            "source_vote": True,
            "destination_vote": True,
            "source_ack": True,
            "destination_ack": False,
            "response": None,
            "error_message": None,
        }
        app = create_app(
            {
                "shard-a": ["http://a1:8000"],
                "shard-b": ["http://b1:8000"],
            },
            transport=httpx.MockTransport(lambda request: httpx.Response(200)),
            coordinator_store=coordinator,
        )

        with TestClient(app) as client:
            response = client.get("/transactions/pending-transfer")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "PENDING")
        self.assertEqual(response.json()["state"], "COMMIT")
        self.assertTrue(response.json()["source_ack"])
        self.assertFalse(response.json()["destination_ack"])

    def test_transaction_status_returns_unknown_for_unseen_transaction(self) -> None:
        app = create_app(
            {
                "shard-a": ["http://a1:8000"],
                "shard-b": ["http://b1:8000"],
            },
            transport=httpx.MockTransport(lambda request: httpx.Response(200)),
            coordinator_store=MemoryCoordinator(),
        )

        with TestClient(app) as client:
            response = client.get("/transactions/not-yet-created")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "transaction_id": "not-yet-created",
                "status": "UNKNOWN",
                "state": None,
                "message": "No coordinated transaction exists for this ID.",
            },
        )

    def test_transaction_status_returns_aborted_for_resolved_abort(self) -> None:
        coordinator = MemoryCoordinator()
        coordinator.transaction = {
            "transaction_id": "aborted-transfer",
            "state": "ABORTED",
            "source_vote": True,
            "destination_vote": False,
            "source_ack": True,
            "destination_ack": True,
            "response": None,
            "error_message": "participant_voted_no",
        }
        app = create_app(
            {
                "shard-a": ["http://a1:8000"],
                "shard-b": ["http://b1:8000"],
            },
            transport=httpx.MockTransport(lambda request: httpx.Response(200)),
            coordinator_store=coordinator,
        )

        with TestClient(app) as client:
            response = client.get("/transactions/aborted-transfer")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ABORTED")
        self.assertEqual(response.json()["state"], "ABORTED")

    def test_cross_shard_transfer_runs_prepare_then_durable_commit(self) -> None:
        coordinator = MemoryCoordinator()
        events: list[str] = []

        def handle(request: httpx.Request) -> httpx.Response:
            host = request.url.host
            if request.url.path == "/health":
                return httpx.Response(200)
            phase = request.url.path.rsplit("/", 1)[-1]
            events.append(f"{phase}:{host}")
            if phase == "prepare":
                return httpx.Response(200, json={"vote": "YES"})
            role = "debit" if host.startswith("a") else "credit"
            balance = 95.0 if role == "debit" else 105.0
            return httpx.Response(
                200,
                json={
                    "transaction_id": "cross-shard-1",
                    "role": role,
                    "balance": balance,
                },
            )

        app = create_app(
            {
                "shard-a": ["http://a1:8000", "http://a2:8000"],
                "shard-b": ["http://b1:8000", "http://b2:8000"],
            },
            transport=httpx.MockTransport(handle),
            coordinator_store=coordinator,
        )
        with TestClient(app) as client:
            response = client.post(
                "/transactions",
                headers={"X-Request-ID": "cross-shard-1"},
                json={
                    "action": "transfer",
                    "account": "A0001",
                    "destination": "A1001",
                    "amount": "5",
                },
            )
            status_response = client.get("/transactions/cross-shard-1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["source_balance"], 95.0)
        self.assertEqual(response.json()["destination_balance"], 105.0)
        self.assertLess(events.index("prepare:a1"), events.index("prepare:b1"))
        self.assertLess(
            coordinator.events.index("durable:COMMIT"),
            coordinator.events.index("ack:source"),
        )
        self.assertLess(
            coordinator.events.index("durable:COMMIT"),
            coordinator.events.index("ack:destination"),
        )
        self.assertEqual(coordinator.transaction["state"], "COMMITTED")
        self.assertEqual(status_response.status_code, 200)
        self.assertEqual(status_response.json()["status"], "COMPLETED")
        self.assertLess(
            coordinator.events.index("lock:cross-shard-1"),
            coordinator.events.index("durable:COMMIT"),
        )
        self.assertEqual(coordinator.events[-1], "unlock:cross-shard-1")

    def test_cross_shard_transfer_owned_by_another_gateway_is_not_run(self) -> None:
        coordinator = MemoryCoordinator()
        coordinator.locked_elsewhere.add("owned-elsewhere")
        participant_calls: list[str] = []

        def handle(request: httpx.Request) -> httpx.Response:
            if request.url.path != "/health":
                participant_calls.append(request.url.path)
            return httpx.Response(200, json={"vote": "YES"})

        app = create_app(
            {
                "shard-a": ["http://a1:8000"],
                "shard-b": ["http://b1:8000"],
            },
            transport=httpx.MockTransport(handle),
            coordinator_store=coordinator,
        )
        with TestClient(app) as client:
            response = client.post(
                "/transactions",
                headers={"X-Request-ID": "owned-elsewhere"},
                json={
                    "action": "transfer",
                    "account": "A0001",
                    "destination": "A1001",
                    "amount": "5",
                },
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json()["detail"]["error"], "transaction_in_progress")
        self.assertEqual(participant_calls, [])
        self.assertIsNone(coordinator.transaction)

    def test_recovery_skips_transaction_owned_by_live_gateway(self) -> None:
        coordinator = MemoryCoordinator()
        coordinator.transaction = {
            "transaction_id": "live-transfer",
            "source_account": "A0001",
            "destination_account": "A1001",
            "amount": "5.00",
            "state": "PREPARING",
            "source_vote": True,
            "destination_vote": None,
            "source_ack": False,
            "destination_ack": False,
            "source_result": None,
            "destination_result": None,
            "response": None,
            "error_message": None,
        }
        coordinator.locked_elsewhere.add("live-transfer")
        app = create_app(
            {
                "shard-a": ["http://a1:8000"],
                "shard-b": ["http://b1:8000"],
            },
            transport=httpx.MockTransport(lambda request: httpx.Response(200)),
            coordinator_store=coordinator,
        )
        with TestClient(app):
            time.sleep(0.3)
            self.assertEqual(coordinator.transaction["state"], "PREPARING")
            coordinator.locked_elsewhere.clear()
            deadline = time.monotonic() + 5
            while (
                coordinator.transaction["state"] == "PREPARING"
                and time.monotonic() < deadline
            ):
                time.sleep(0.1)

        self.assertIn("durable:ABORT", coordinator.events)
        self.assertNotIn("durable:COMMIT", coordinator.events)

    def test_gateway_discovers_replicas_from_service_dns(self) -> None:
        def handle(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/health":
                return httpx.Response(200)
            return httpx.Response(
                200,
                json={"host": request.url.host},
                headers={"X-Served-By": "shard-a-app-7f9c"},
            )

        app = create_app(
            transport=httpx.MockTransport(handle),
            shard_services={
                "shard-a": ("localhost", 8000),
                "shard-b": ("localhost", 8001),
            },
        )
        with TestClient(app) as client:
            response = client.get("/balance/inquiry?account=A0002")
            metrics = client.get("/metrics").text

        self.assertEqual(response.status_code, 200)
        self.assertIn(response.json()["host"], {"127.0.0.1", "::1"})
        self.assertEqual(response.headers["x-backend-replica"], "shard-a-app-7f9c")
        self.assertIn('bank_gateway_discovered_replicas{shard="shard-a"}', metrics)

    def test_gateway_reports_unresolvable_shard_service_as_unavailable(self) -> None:
        app = create_app(
            transport=httpx.MockTransport(lambda request: httpx.Response(200)),
            shard_services={
                "shard-a": ("no-ready-pods.invalid", 8000),
                "shard-b": ("localhost", 8001),
            },
        )
        with TestClient(app) as client:
            failed = client.get("/balance/inquiry?account=A0001")
            healthy_shard = client.get("/balance/inquiry?account=A1001")
            ready = client.get("/readyz")

        self.assertEqual(failed.status_code, 503)
        self.assertEqual(failed.json()["detail"]["error"], "shard_unavailable")
        self.assertEqual(healthy_shard.status_code, 200)
        self.assertEqual(ready.status_code, 200)


if __name__ == "__main__":
    unittest.main()
