import os
import time
import unittest

from models import CoordinatorStore


class CoordinatorTransactionLockTests(unittest.TestCase):
    def setUp(self) -> None:
        database_url = os.getenv("BANK_TEST_DATABASE_URL")
        if not database_url:
            self.skipTest(
                "Set BANK_TEST_DATABASE_URL to a dedicated PostgreSQL test database."
            )
        self.first_gateway = CoordinatorStore(database_url)
        self.second_gateway = CoordinatorStore(database_url)
        self.addCleanup(self.first_gateway.engine.dispose)
        self.addCleanup(self.second_gateway.engine.dispose)

    def test_only_one_gateway_owns_a_transaction(self) -> None:
        owner = self.first_gateway.acquire_transaction_lock("lock-test-1", 1)
        self.assertIsNotNone(owner)
        try:
            self.assertIsNone(
                self.second_gateway.acquire_transaction_lock("lock-test-1", 0))
            started = time.monotonic()
            self.assertIsNone(
                self.second_gateway.acquire_transaction_lock("lock-test-1", 0.3))
            self.assertGreaterEqual(time.monotonic() - started, 0.25)
            other = self.second_gateway.acquire_transaction_lock("lock-test-2", 0)
            self.assertIsNotNone(other)
            self.second_gateway.release_transaction_lock(other)
        finally:
            self.first_gateway.release_transaction_lock(owner)

    def test_backup_barrier_blocks_new_owners_but_not_existing_ones(self) -> None:
        owner = self.first_gateway.acquire_transaction_lock("lock-test-4", 1)
        self.assertIsNotNone(owner)
        backup = self.second_gateway.lock_engine.connect()
        try:
            backup.exec_driver_sql("SET lock_timeout = '300ms'")
            with self.assertRaises(Exception):
                backup.exec_driver_sql("SELECT pg_advisory_lock(7420, 1)")
            backup.rollback()
            self.first_gateway.release_transaction_lock(owner)
            backup.exec_driver_sql("SELECT pg_advisory_lock(7420, 1)")
            backup.commit()
            self.assertIsNone(
                self.first_gateway.acquire_transaction_lock("lock-test-5", 0))
            self.assertIsNone(
                self.first_gateway.acquire_transaction_lock("lock-test-5", 0.2))
        finally:
            backup.close()
        resumed = self.first_gateway.acquire_transaction_lock("lock-test-5", 1)
        self.assertIsNotNone(resumed)
        self.first_gateway.release_transaction_lock(resumed)

    def test_lock_is_released_when_owner_connection_ends(self) -> None:
        owner = self.first_gateway.acquire_transaction_lock("lock-test-3", 1)
        self.assertIsNotNone(owner)
        owner.invalidate()
        recovered = self.second_gateway.acquire_transaction_lock("lock-test-3", 2)
        self.assertIsNotNone(recovered)
        self.second_gateway.release_transaction_lock(recovered)


if __name__ == "__main__":
    unittest.main()
