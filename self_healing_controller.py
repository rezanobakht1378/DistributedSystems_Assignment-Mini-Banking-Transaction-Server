from __future__ import annotations

import logging
import os
import re
import time
from typing import Any

import docker
import httpx
from docker.errors import APIError, DockerException, NotFound
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from models import CoordinatorStore

_LOG = logging.getLogger("banking_self_healing")
_SLOT_PATTERN = re.compile(r"replacement-(\d+)\Z")


class SelfHealingController:
    def __init__(
        self,
        coordinator: CoordinatorStore,
        docker_client: Any,
        *,
        image: str,
        gateway_container: str,
        databases: dict[str, str],
        desired_replicas: int = 2,
        interval: float = 3,
    ) -> None:
        if desired_replicas < 2:
            raise ValueError("At least two healthy replicas per shard are required.")
        if interval <= 0:
            raise ValueError("Controller interval must be positive.")
        self.coordinator = coordinator
        self.docker = docker_client
        self.image = image
        self.gateway_container = gateway_container
        self.databases = databases
        self.desired_replicas = desired_replicas
        self.interval = interval
        self._http = httpx.Client(timeout=1)

    def close(self) -> None:
        self._http.close()
        self.docker.close()

    def run_forever(self) -> None:
        lock_connection = self.coordinator.engine.connect()
        try:
            owns_lock = lock_connection.scalar(
                text(
                    "SELECT pg_try_advisory_lock("
                    "hashtextextended('banking-self-healing-controller', 0))"
                )
            )
            if not owns_lock:
                _LOG.error(
                    "Another self-healing controller owns the coordinator lock."
                )
                return
            _LOG.info("Self-healing controller acquired its durable singleton lock.")
            while True:
                try:
                    self.reconcile_once()
                except (APIError, DockerException, OSError, RuntimeError, SQLAlchemyError):
                    _LOG.exception("Self-healing reconciliation failed.")
                time.sleep(self.interval)
        finally:
            try:
                lock_connection.execute(
                    text(
                        "SELECT pg_advisory_unlock("
                        "hashtextextended('banking-self-healing-controller', 0))"
                    )
                )
                lock_connection.commit()
            finally:
                lock_connection.close()

    def reconcile_once(self) -> None:
        for shard_id, database_url in sorted(self.databases.items()):
            records = self.coordinator.replica_records(shard_id)
            healthy_count = 0
            for replica in records:
                healthy = self._healthy(replica["url"])
                self.coordinator.set_replica_enabled(
                    shard_id,
                    replica["replica_id"],
                    healthy,
                )
                if healthy:
                    healthy_count += 1
                elif replica["managed"]:
                    self._remove_managed_container(replica["container_name"])

            for replica in records:
                if healthy_count <= self.desired_replicas:
                    break
                if replica["managed"] and self._healthy(replica["url"]):
                    self.coordinator.set_replica_enabled(
                        shard_id,
                        replica["replica_id"],
                        False,
                    )
                    self._remove_managed_container(replica["container_name"])
                    healthy_count -= 1

            while healthy_count < self.desired_replicas:
                record = self._next_managed_record(shard_id)
                self._start_or_recover(
                    shard_id,
                    database_url,
                    record,
                )
                if not self._healthy(record["url"]):
                    raise RuntimeError(
                        f"Replacement {record['container_name']} for {shard_id} "
                        "did not become healthy."
                    )
                self.coordinator.set_replica_enabled(
                    shard_id,
                    record["replica_id"],
                    True,
                )
                healthy_count += 1

    def _healthy(self, url: str) -> bool:
        try:
            response = self._http.get(url.rstrip("/") + "/health")
        except httpx.RequestError:
            return False
        return response.is_success

    def _next_managed_record(self, shard_id: str) -> dict[str, Any]:
        managed = [
            record
            for record in self.coordinator.replica_records(shard_id)
            if record["managed"]
        ]
        for record in managed:
            if not self._healthy(record["url"]):
                return record

        used_slots = [
            int(match.group(1))
            for record in managed
            if (match := _SLOT_PATTERN.fullmatch(record["replica_id"]))
        ]
        slot = max(used_slots, default=0) + 1
        replica_id = f"replacement-{slot}"
        container_name = f"mini-banking-managed-{shard_id}-{slot}"
        return {
            "shard_id": shard_id,
            "replica_id": replica_id,
            "url": f"http://{container_name}:8000",
            "container_name": container_name,
            "managed": True,
            "enabled": False,
        }

    def _network_name(self) -> str:
        gateway = self.docker.containers.get(self.gateway_container)
        gateway.reload()
        networks = gateway.attrs["NetworkSettings"]["Networks"]
        if not networks:
            raise RuntimeError("Gateway is not attached to a Docker network.")
        return next(iter(networks))

    def _remove_managed_container(self, container_name: str | None) -> None:
        if not container_name:
            raise RuntimeError("Managed replica is missing its container name.")
        try:
            self.docker.containers.get(container_name).remove(force=True)
            _LOG.info("Removed managed replica container %s.", container_name)
        except NotFound:
            pass

    def _start_or_recover(
        self,
        shard_id: str,
        database_url: str,
        record: dict[str, Any],
    ) -> None:
        container_name = record["container_name"]
        self.coordinator.register_replica(
            shard_id,
            record["replica_id"],
            record["url"],
            container_name=container_name,
            managed=True,
            enabled=False,
        )
        try:
            container = self.docker.containers.get(container_name)
            container.reload()
            self._attach_to_gateway_network(container)
            if container.status == "running":
                return
            container.start()
            _LOG.info("Restarted persisted managed replica %s.", container_name)
            return
        except NotFound:
            pass

        labels = {
            "banking.managed": "true",
            "banking.shard": shard_id,
            "banking.replica-id": record["replica_id"],
        }
        container = self.docker.containers.create(
            self.image,
            command=["python", "main.py"],
            name=container_name,
            environment={
                "BANK_SHARD_ID": shard_id,
                "BANK_HOST": "0.0.0.0",
                "BANK_PORT": "8000",
                "DATABASE_URL": database_url,
                "BANK_LOG_FILE": "/tmp/banking_server.log",
            },
            labels=labels,
            restart_policy={"Name": "no"},
        )
        self._attach_to_gateway_network(container)
        container.start()
        _LOG.info(
            "Created managed replica %s for %s without an automatic restart policy.",
            container_name,
            shard_id,
        )
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if self._healthy(record["url"]):
                return
            time.sleep(1)
        raise RuntimeError(f"Managed replica {container_name} failed its health check.")

    def _attach_to_gateway_network(self, container: Any) -> None:
        container.reload()
        network_name = self._network_name()
        attached_networks = container.attrs["NetworkSettings"]["Networks"]
        if network_name not in attached_networks:
            network = self.docker.networks.get(network_name)
            network.connect(
                container,
                aliases=[container.name],
            )


def main() -> None:
    logging.basicConfig(
        level=os.getenv("BANK_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    coordinator_url = os.environ["BANK_COORDINATOR_DATABASE_URL"]
    database_urls = {
        "shard-a": os.environ["BANK_SHARD_A_DATABASE_URL"],
        "shard-b": os.environ["BANK_SHARD_B_DATABASE_URL"],
    }
    desired_replicas = int(os.getenv("BANK_DESIRED_REPLICAS_PER_SHARD", "2"))
    coordinator = CoordinatorStore(coordinator_url)
    coordinator.initialize()
    controller = SelfHealingController(
        coordinator,
        docker.from_env(),
        image=os.getenv("BANK_APP_IMAGE", "mini-banking-app:local"),
        gateway_container=os.getenv(
            "BANK_GATEWAY_CONTAINER_NAME",
            "mini-banking-gateway",
        ),
        databases=database_urls,
        desired_replicas=desired_replicas,
        interval=float(os.getenv("BANK_HEALING_INTERVAL_SECONDS", "3")),
    )
    try:
        controller.run_forever()
    finally:
        controller.close()
        coordinator.engine.dispose()


if __name__ == "__main__":
    main()
