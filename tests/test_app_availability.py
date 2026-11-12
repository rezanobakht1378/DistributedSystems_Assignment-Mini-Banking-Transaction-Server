import dataclasses
import os
import tempfile
import threading
import time
import unittest

from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from main import Settings, _is_connection_failure, create_app


class _DriverError(Exception):
    def __init__(self, pgcode: str | None) -> None:
        super().__init__(pgcode)
        self.pgcode = pgcode


class DatabaseErrorClassificationTests(unittest.TestCase):
    def test_lock_and_pool_timeouts_are_overload_not_outage(self) -> None:
        for pgcode in ("55P03", "57014", "25P03", "40P01", "40001"):
            with self.subTest(pgcode=pgcode):
                error = OperationalError("SELECT 1", {}, _DriverError(pgcode))
                self.assertFalse(_is_connection_failure(error))
        self.assertFalse(_is_connection_failure(SQLAlchemyTimeoutError("pool exhausted")))

    def test_connection_errors_are_outages(self) -> None:
        for pgcode in (None, "08006", "57P01"):
            with self.subTest(pgcode=pgcode):
                error = OperationalError("SELECT 1", {}, _DriverError(pgcode))
                self.assertTrue(_is_connection_failure(error))


class ApplicationAvailabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        log_directory = tempfile.mkdtemp()
        self.settings = Settings(
            database_url="postgresql://banking:banking@127.0.0.1:1/unreachable",
            max_clients=10,
            role="primary",
            node_id="shard-a-app-test",
            peer_url=None,
            primary_url=None,
            heartbeat_interval=1,
            failure_threshold=3,
            log_file=os.path.join(log_directory, "banking_server.log"),
            shard_id="shard-a",
            database_check_interval=0.1,
        )

    def test_database_outage_makes_replica_unready_but_alive(self) -> None:
        with TestClient(create_app(self.settings)) as client:
            time.sleep(0.3)
            liveness = client.get("/livez")
            readiness = client.get("/readyz")
            deposit = client.post(
                "/transactions",
                headers={"X-Request-ID": "db-down-deposit"},
                json={"action": "deposit", "account": "A0001", "amount": "5"},
            )
            metrics = client.get("/metrics").text

        self.assertEqual(liveness.status_code, 200)
        self.assertEqual(readiness.status_code, 503)
        self.assertEqual(readiness.json()["database"], "down")
        self.assertEqual(deposit.status_code, 503)
        self.assertEqual(deposit.json()["error"], "database_unavailable")
        self.assertEqual(deposit.headers["x-served-by"], "shard-a-app-test")
        self.assertIn('bank_db_up{shard="shard-a"} 0.0', metrics)

    def test_probes_answer_when_all_request_slots_are_taken(self) -> None:
        app = create_app(dataclasses.replace(self.settings, max_clients=1))
        entered = threading.Event()
        release = threading.Event()

        @app.get("/test/block")
        def block() -> dict[str, str]:
            entered.set()
            release.wait(10)
            return {"status": "done"}

        with TestClient(app) as client:
            blocked = threading.Thread(target=client.get, args=("/test/block",))
            blocked.start()
            try:
                self.assertTrue(entered.wait(5))
                busy = client.get("/balance/inquiry?account=A0001")
                liveness = client.get("/livez")
                readiness = client.get("/readyz")
            finally:
                release.set()
                blocked.join(10)

        self.assertEqual(busy.status_code, 503)
        self.assertEqual(busy.json()["error"], "server_busy")
        self.assertEqual(liveness.status_code, 200)
        self.assertEqual(readiness.json()["database"], "down")

    def test_wrong_shard_is_rejected_before_database_access(self) -> None:
        with TestClient(create_app(self.settings)) as client:
            response = client.get("/balance/inquiry?account=A1001")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error"], "account_not_on_shard")


if __name__ == "__main__":
    unittest.main()
