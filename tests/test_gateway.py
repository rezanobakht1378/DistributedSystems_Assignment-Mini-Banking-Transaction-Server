import unittest

import httpx
from fastapi.testclient import TestClient

from gateway import create_app


class MemoryCoordinator:
    def __init__(self) -> None:
        self.transaction: dict[str, object] | None = None
        self.events: list[str] = []

    def initialize(self) -> None:
        pass

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
        if self.transaction is None:
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
        if self.transaction["source_ack"] and self.transaction["destination_ack"]:
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
        return []


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


if __name__ == "__main__":
    unittest.main()
