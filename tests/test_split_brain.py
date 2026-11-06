from decimal import Decimal
import logging
import os
import unittest
import uuid
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fastapi import HTTPException
import psycopg2
from psycopg2 import sql

from models import (
    BankStore,
    BankWitness,
    CoordinatorStore,
    Operation,
    ShardedBankStore,
)


class SplitBrainWitnessTests(unittest.TestCase):
    def setUp(self) -> None:
        database_url = os.getenv("BANK_TEST_DATABASE_URL")
        if not database_url:
            self.skipTest(
                "Set BANK_TEST_DATABASE_URL to a dedicated PostgreSQL test database."
            )

        self.database_url = database_url
        self.schemas: list[str] = []
        self.addCleanup(self._drop_schemas)
        account_a_url = self._create_schema_url()
        account_b_url = self._create_schema_url()
        self.witness_url = self._create_schema_url()
        coordinator_url = self._create_schema_url()
        self.store_a = BankStore(account_a_url)
        self.store_b = BankStore(account_b_url)
        self.coordinator = CoordinatorStore(coordinator_url)
        self.witness_a = BankWitness(
            self.witness_url,
            "split-brain-test",
            logging.getLogger("split_brain_test"),
        )
        self.witness_b = BankWitness(
            self.witness_url,
            "split-brain-test",
            logging.getLogger("split_brain_test"),
        )
        self.addCleanup(self.store_a.engine.dispose)
        self.addCleanup(self.store_b.engine.dispose)
        self.addCleanup(self.coordinator.engine.dispose)
        self.addCleanup(self.witness_a.close)
        self.addCleanup(self.witness_b.close)

        self.store_a.initialize()
        self.store_b.initialize()
        self.coordinator.initialize()
        self.witness_a.initialize()

    def _create_schema_url(self) -> str:
        schema_name = f"bank_test_{uuid.uuid4().hex}"
        connection = psycopg2.connect(self.database_url)
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
                )
        finally:
            connection.close()
        self.schemas.append(schema_name)

        database_url = urlsplit(self.database_url)
        query = dict(parse_qsl(database_url.query, keep_blank_values=True))
        query["options"] = f"-c search_path={schema_name}"
        return urlunsplit(
            database_url._replace(query=urlencode(query))
        )

    def _drop_schemas(self) -> None:
        connection = psycopg2.connect(self.database_url)
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                for schema_name in self.schemas:
                    cursor.execute(
                        sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                            sql.Identifier(schema_name)
                        )
                    )
        finally:
            connection.close()

    def test_partition_blocks_promotion_and_rejoining_primary_catches_up(self) -> None:
        cluster_id = "split-brain-test"
        first_epoch = self.witness_a.try_acquire(
            "server-a",
            self.store_a.replica_epoch(cluster_id),
            allow_initial=True,
        )
        self.assertEqual(first_epoch, 1)
        self.store_a.set_replica_epoch(cluster_id, first_epoch)
        first_result = self.store_a.execute(
            Operation("deposit", "A1001", Decimal("5.00")),
            "confirmed-deposit",
            "127.0.0.1",
            cluster_id,
            first_epoch,
        )
        self.store_b.restore_snapshot(
            self.store_a.export_snapshot(),
            cluster_id,
            first_epoch,
        )
        duplicate_result = self.store_b.execute(
            Operation("deposit", "A1001", Decimal("5.00")),
            "confirmed-deposit",
            "127.0.0.1",
            cluster_id,
            first_epoch,
        )
        self.assertEqual(duplicate_result, first_result)
        self.assertEqual(self.store_b.get_balance("A1001")["balance"], 1005.0)

        self.assertIsNone(
            self.witness_b.try_acquire(
                "server-b",
                self.store_b.replica_epoch(cluster_id),
            )
        )

        self.witness_a.close()
        second_epoch = self.witness_b.try_acquire(
            "server-b",
            self.store_b.replica_epoch(cluster_id),
        )
        self.assertEqual(second_epoch, 2)
        self.store_b.set_replica_epoch(cluster_id, second_epoch)
        self.witness_a = BankWitness(
            self.witness_url,
            cluster_id,
            logging.getLogger("split_brain_test"),
        )
        self.addCleanup(self.witness_a.close)

        self.assertIsNone(
            self.witness_a.try_acquire(
                "server-a",
                self.store_a.replica_epoch(cluster_id),
            )
        )
        self.witness_b.close()
        self.assertIsNone(
            self.witness_a.try_acquire("server-a", first_epoch)
        )

        self.store_a.restore_snapshot(
            self.store_b.export_snapshot(),
            cluster_id,
            second_epoch,
        )
        third_epoch = self.witness_a.try_acquire(
            "server-a",
            self.store_a.replica_epoch(cluster_id),
        )
        self.assertEqual(third_epoch, 3)
        self.assertEqual(
            self.store_a.get_balance("A1001")["balance"],
            1005.0,
        )

    def test_rejected_stale_candidate_releases_witness_lock(self) -> None:
        cluster_id = "stale-lock-test"
        first_witness = BankWitness(
            self.witness_url,
            cluster_id,
            logging.getLogger("split_brain_test"),
        )
        candidate_witness = BankWitness(
            self.witness_url,
            cluster_id,
            logging.getLogger("split_brain_test"),
        )
        self.addCleanup(first_witness.close)
        self.addCleanup(candidate_witness.close)
        first_epoch = first_witness.try_acquire(
            "server-a",
            None,
            allow_initial=True,
        )
        self.assertEqual(first_epoch, 1)
        first_witness.close()

        self.assertIsNone(candidate_witness.try_acquire("server-b", None))

        recovery_witness = BankWitness(
            self.witness_url,
            cluster_id,
            logging.getLogger("split_brain_test"),
        )
        self.addCleanup(recovery_witness.close)
        self.assertEqual(recovery_witness.try_acquire("server-a", 1), 2)

    def test_transfer_commits_both_accounts_and_is_idempotent(self) -> None:
        operation = Operation(
            "transfer",
            "A1001",
            Decimal("25.00"),
            "A1002",
        )
        result = self.store_a.execute(
            operation,
            "confirmed-transfer",
            "127.0.0.1",
        )
        replay = self.store_a.execute(
            operation,
            "confirmed-transfer",
            "127.0.0.1",
        )
        replica_result = self.store_b.execute(
            operation,
            "confirmed-transfer",
            "127.0.0.1",
        )

        self.assertEqual(result, replay)
        self.assertEqual(result, replica_result)
        self.assertEqual(self.store_a.get_balance("A1001")["balance"], 975.0)
        self.assertEqual(self.store_a.get_balance("A1002")["balance"], 525.0)
        self.assertEqual(self.store_b.get_balance("A1001")["balance"], 975.0)
        self.assertEqual(self.store_b.get_balance("A1002")["balance"], 525.0)

    def test_failed_transfer_leaves_both_accounts_unchanged(self) -> None:
        before_source = self.store_a.get_balance("A1001")["balance"]
        before_destination = self.store_a.get_balance("A1002")["balance"]
        with self.assertRaises(HTTPException):
            self.store_a.execute(
                Operation(
                    "transfer",
                    "A1001",
                    Decimal("5000.00"),
                    "A1002",
                ),
                "failed-transfer",
                "127.0.0.1",
            )

        self.assertEqual(
            self.store_a.get_balance("A1001")["balance"],
            before_source,
        )
        self.assertEqual(
            self.store_a.get_balance("A1002")["balance"],
            before_destination,
        )

    def test_sharded_same_shard_transfer_commits_atomically(self) -> None:
        sharded_store = ShardedBankStore.__new__(ShardedBankStore)
        sharded_store.stores = {
            "shard-a": self.store_a,
            "shard-b": self.store_b,
        }
        source_before = self.store_a.get_balance("A0001")["balance"]
        destination_before = self.store_a.get_balance("A0002")["balance"]
        total_before = source_before + destination_before

        result = sharded_store.execute(
            Operation(
                "transfer",
                "A0001",
                Decimal("25.00"),
                "A0002",
            ),
            "sharded-same-shard-transfer",
            "127.0.0.1",
        )

        self.assertEqual(result["source_balance"], source_before - 25.0)
        self.assertEqual(
            result["destination_balance"],
            destination_before + 25.0,
        )
        self.assertEqual(
            self.store_a.get_balance("A0001")["balance"]
            + self.store_a.get_balance("A0002")["balance"],
            total_before,
        )
        self.assertEqual(
            self.store_b.get_balance("A0001")["balance"],
            source_before,
        )

    def test_sharded_same_shard_transfer_rolls_back_both_accounts(self) -> None:
        sharded_store = ShardedBankStore.__new__(ShardedBankStore)
        sharded_store.stores = {
            "shard-a": self.store_a,
            "shard-b": self.store_b,
        }
        source_before = self.store_a.get_balance("A0001")["balance"]
        destination_before = self.store_a.get_balance("A0002")["balance"]

        with self.assertRaises(HTTPException):
            sharded_store.execute(
                Operation(
                    "transfer",
                    "A0001",
                    Decimal("10000.00"),
                    "A0002",
                ),
                "sharded-failed-same-shard-transfer",
                "127.0.0.1",
            )

        self.assertEqual(
            self.store_a.get_balance("A0001")["balance"],
            source_before,
        )
        self.assertEqual(
            self.store_a.get_balance("A0002")["balance"],
            destination_before,
        )

    def test_cross_shard_participants_reserve_then_commit_idempotently(self) -> None:
        amount = Decimal("25.00")
        source_before = self.store_a.get_balance("A0001")["balance"]
        destination_before = self.store_b.get_balance("A1001")["balance"]

        source_vote = self.store_a.prepare_cross_shard_transfer(
            "cross-commit",
            "A0001",
            "A1001",
            amount,
            "debit",
            "127.0.0.1",
        )
        destination_vote = self.store_b.prepare_cross_shard_transfer(
            "cross-commit",
            "A0001",
            "A1001",
            amount,
            "credit",
            "127.0.0.1",
        )
        self.assertEqual(source_vote["vote"], "YES")
        self.assertEqual(destination_vote["vote"], "YES")

        with self.assertRaises(HTTPException):
            self.store_a.execute(
                Operation("withdraw", "A0001", Decimal("730.00")),
                "reserved-withdraw",
                "127.0.0.1",
            )

        source_result = self.store_a.commit_cross_shard_transfer("cross-commit")
        destination_result = self.store_b.commit_cross_shard_transfer(
            "cross-commit"
        )
        self.assertEqual(
            self.store_a.commit_cross_shard_transfer("cross-commit"),
            source_result,
        )
        self.assertEqual(
            self.store_b.commit_cross_shard_transfer("cross-commit"),
            destination_result,
        )
        self.assertEqual(
            self.store_a.get_balance("A0001")["balance"],
            source_before - 25.0,
        )
        self.assertEqual(
            self.store_b.get_balance("A1001")["balance"],
            destination_before + 25.0,
        )

    def test_cross_shard_abort_releases_participant_reservations(self) -> None:
        amount = Decimal("25.00")
        source_before = self.store_a.get_balance("A0001")["balance"]
        destination_before = self.store_b.get_balance("A1001")["balance"]
        for store, role in (
            (self.store_a, "debit"),
            (self.store_b, "credit"),
        ):
            self.assertEqual(
                store.prepare_cross_shard_transfer(
                    "cross-abort",
                    "A0001",
                    "A1001",
                    amount,
                    role,
                    "127.0.0.1",
                )["vote"],
                "YES",
            )
        for store, role in (
            (self.store_a, "debit"),
            (self.store_b, "credit"),
        ):
            self.assertEqual(
                store.abort_cross_shard_transfer(
                    "cross-abort",
                    "A0001",
                    "A1001",
                    amount,
                    role,
                    "127.0.0.1",
                )["state"],
                "aborted",
            )
        self.store_a.execute(
            Operation(
                "withdraw",
                "A0001",
                Decimal(str(source_before)),
            ),
            "withdraw-after-abort",
            "127.0.0.1",
        )
        self.assertEqual(self.store_a.get_balance("A0001")["balance"], 0.0)
        self.assertEqual(
            self.store_b.get_balance("A1001")["balance"],
            destination_before,
        )

    def test_coordinator_logs_durable_commit_before_acknowledgements(self) -> None:
        operation = Operation(
            "transfer",
            "A0001",
            Decimal("25.00"),
            "A1001",
        )
        self.coordinator.begin(
            "coordinated-commit",
            operation.fingerprint(),
            "A0001",
            "A1001",
            Decimal("25.00"),
        )
        self.coordinator.record_vote("coordinated-commit", "source", True)
        self.coordinator.record_vote(
            "coordinated-commit",
            "destination",
            True,
        )
        decision = self.coordinator.decide("coordinated-commit", "COMMIT")
        self.assertEqual(decision["state"], "COMMIT")
        with self.assertRaises(RuntimeError):
            self.coordinator.decide("coordinated-commit", "ABORT")

        self.coordinator.record_ack(
            "coordinated-commit",
            "source",
            {"balance": 725.0},
        )
        completed = self.coordinator.record_ack(
            "coordinated-commit",
            "destination",
            {"balance": 1025.0},
        )
        self.assertEqual(completed["state"], "COMMITTED")
        self.assertEqual(completed["response"]["source_balance"], 725.0)
        self.assertEqual(
            completed["response"]["destination_balance"],
            1025.0,
        )


if __name__ == "__main__":
    unittest.main()
