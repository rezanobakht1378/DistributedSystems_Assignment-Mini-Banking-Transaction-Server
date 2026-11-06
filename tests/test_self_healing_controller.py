import unittest
from unittest.mock import MagicMock

from self_healing_controller import SelfHealingController


class SelfHealingControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = MagicMock()
        self.docker = MagicMock()
        self.controller = SelfHealingController(
            self.store,
            self.docker,
            image="mini-banking-app:local",
            gateway_container="mini-banking-gateway",
            databases={"shard-a": "postgresql://db/bank"},
        )
        self.client = MagicMock()
        self.controller._http = self.client
        self.addCleanup(self.controller.close)

    def test_detects_failed_replica_and_selects_persistent_replacement_slot(self) -> None:
        self.store.replica_records.return_value = [
            {
                "shard_id": "shard-a",
                "replica_id": "shard-a-app-1",
                "url": "http://shard-a-app-1:8000",
                "container_name": None,
                "managed": False,
                "enabled": False,
            },
            {
                "shard_id": "shard-a",
                "replica_id": "replacement-2",
                "url": "http://mini-banking-managed-shard-a-2:8000",
                "container_name": "mini-banking-managed-shard-a-2",
                "managed": True,
                "enabled": False,
            },
        ]
        self.client.get.return_value.is_success = False

        record = self.controller._next_managed_record("shard-a")

        self.assertEqual(record["replica_id"], "replacement-2")
        self.assertEqual(
            record["container_name"],
            "mini-banking-managed-shard-a-2",
        )

    def test_allocates_next_managed_slot_without_duplicate_names(self) -> None:
        self.store.replica_records.return_value = [
            {
                "shard_id": "shard-a",
                "replica_id": "replacement-1",
                "url": "http://mini-banking-managed-shard-a-1:8000",
                "container_name": "mini-banking-managed-shard-a-1",
                "managed": True,
                "enabled": True,
            }
        ]
        self.client.get.return_value.is_success = True

        record = self.controller._next_managed_record("shard-a")

        self.assertEqual(record["replica_id"], "replacement-2")

    def test_recovers_persisted_container_before_starting_it(self) -> None:
        container = MagicMock()
        container.status = "exited"
        self.docker.containers.get.return_value = container
        order: list[str] = []
        self.controller._attach_to_gateway_network = MagicMock(
            side_effect=lambda current: order.append("network")
        )
        container.start.side_effect = lambda: order.append("start")
        record = {
            "replica_id": "replacement-1",
            "url": "http://replacement:8000",
            "container_name": "replacement",
        }

        self.controller._start_or_recover(
            "shard-a",
            "postgresql://db/bank",
            record,
        )

        self.assertEqual(order, ["network", "start"])

    def test_scales_back_managed_replica_after_static_replica_recovers(self) -> None:
        self.store.replica_records.return_value = [
            {
                "shard_id": "shard-a",
                "replica_id": "shard-a-app-1",
                "url": "http://shard-a-app-1:8000",
                "container_name": None,
                "managed": False,
            },
            {
                "shard_id": "shard-a",
                "replica_id": "shard-a-app-2",
                "url": "http://shard-a-app-2:8000",
                "container_name": None,
                "managed": False,
            },
            {
                "shard_id": "shard-a",
                "replica_id": "replacement-1",
                "url": "http://replacement:8000",
                "container_name": "replacement",
                "managed": True,
            },
        ]
        self.client.get.return_value.is_success = True

        self.controller.reconcile_once()

        self.store.set_replica_enabled.assert_any_call(
            "shard-a",
            "replacement-1",
            False,
        )
        self.docker.containers.get.return_value.remove.assert_called_once_with(
            force=True
        )


if __name__ == "__main__":
    unittest.main()
