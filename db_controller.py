"""Leader record, failover and routing for the PostgreSQL database clusters.

Every database cluster (for example ``shard-a-db``) is a StatefulSet of one
primary and one or more standbys with streaming replication. The Kubernetes
Lease ``<cluster>-leader`` is the single source of truth for which Pod is the
primary; it is only changed with a compare-and-swap on its resourceVersion,
and ``spec.leaseTransitions`` is the leadership epoch.

Exactly one standby is synchronous at a time. The controller names it in the
primary's ``synchronous_standby_names`` and, preferring a standby on another
node, records it in the Lease only after PostgreSQL reports it as ``sync``.
Before switching to another standby it first removes that record, so the
recorded standby always holds every acknowledged commit and is the only
automatic failover target.

Several controller instances may run; only the one holding the
``db-controller`` Lease acts (see ``LeaderElection``).

Commands:
  bootstrap  init container: decide this Pod's role from the Lease
  run        controller loop: label the primary, promote, fail over
  failover   manual failover/switchover of one cluster
  status     print the observed state of all clusters
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import psycopg2
from prometheus_client import Counter, Gauge, start_http_server

_LOG = logging.getLogger("banking_db_controller")
_SERVICE_ACCOUNT = Path("/var/run/secrets/kubernetes.io/serviceaccount")
_ANNOTATION_INITIALIZED = "banking.local/initialized"
_ANNOTATION_SYNCED_STANDBY = "banking.local/synced-standby"
# WAL timeline of the recorded leader. A primary on a newer timeline than the
# record proves the record is stale (e.g. etcd was restored from a snapshot).
_ANNOTATION_TIMELINE = "banking.local/leader-timeline"
# Set by a failover until the new leader runs as primary. Without it, a
# recorded leader that is a standby is never promoted.
_ANNOTATION_PROMOTING = "banking.local/promoting"
_ROLE_LABEL = "db-role"
_HOSTNAME_LABEL = "kubernetes.io/hostname"

POD_UP = Gauge(
    "bank_db_pod_up", "1 when the database Pod accepts SQL connections.",
    ["cluster", "pod"])
POD_PRIMARY = Gauge(
    "bank_db_pod_is_primary", "1 when the database Pod is a writable primary.",
    ["cluster", "pod"])
LEADER_EPOCH = Gauge(
    "bank_db_leader_epoch", "Leadership epoch recorded in the cluster Lease.",
    ["cluster"])
REPLICATION_LAG = Gauge(
    "bank_db_replication_lag_bytes",
    "WAL bytes the standby has not replayed yet.", ["cluster", "standby"])
SYNC_STANDBY = Gauge(
    "bank_db_sync_standby_connected",
    "1 when a synchronous standby is streaming, so commits can be acknowledged.",
    ["cluster"])
WRITABLE = Gauge(
    "bank_db_writable",
    "1 when the recorded leader is a reachable primary with a synchronous standby.",
    ["cluster"])
FAILOVERS = Counter(
    "bank_db_failovers_total", "Completed leader changes.", ["cluster", "reason"])
FENCES = Counter(
    "bank_db_fenced_pods_total",
    "Non-leader Pods deleted because they ran as a primary.", ["cluster"])
REBUILDS = Counter(
    "bank_db_member_rebuilds_total",
    "Standby volumes discarded and re-cloned.", ["cluster", "reason"])
CONTROLLER_LEADING = Gauge(
    "bank_db_controller_leading",
    "1 on the controller instance that holds the db-controller Lease.")


class Conflict(Exception):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class KubernetesApi:
    def __init__(self) -> None:
        host = os.environ["KUBERNETES_SERVICE_HOST"]
        port = os.getenv("KUBERNETES_SERVICE_PORT", "443")
        if ":" in host:
            host = f"[{host}]"
        self.namespace = (_SERVICE_ACCOUNT / "namespace").read_text().strip()
        self.client = httpx.Client(
            base_url=f"https://{host}:{port}",
            verify=str(_SERVICE_ACCOUNT / "ca.crt"),
            timeout=5,
        )

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        headers = kwargs.pop("headers", {})
        token = (_SERVICE_ACCOUNT / "token").read_text().strip()
        headers["Authorization"] = f"Bearer {token}"
        return self.client.request(method, path, headers=headers, **kwargs)

    def _lease_path(self, name: str = "") -> str:
        base = f"/apis/coordination.k8s.io/v1/namespaces/{self.namespace}/leases"
        return f"{base}/{name}" if name else base

    def _pod_path(self, name: str) -> str:
        return f"/api/v1/namespaces/{self.namespace}/pods/{name}"

    def get_lease(self, name: str) -> dict[str, Any] | None:
        response = self._request("GET", self._lease_path(name))
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def create_lease(self, name: str, holder: str) -> dict[str, Any]:
        body = {
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": name,
                "annotations": {_ANNOTATION_INITIALIZED: "false"},
            },
            "spec": {
                "holderIdentity": holder,
                "leaseTransitions": 0,
                "acquireTime": _now(),
            },
        }
        response = self._request("POST", self._lease_path(), json=body)
        if response.status_code == 409:
            raise Conflict(name)
        response.raise_for_status()
        return response.json()

    def replace_lease(self, lease: dict[str, Any]) -> dict[str, Any]:
        response = self._request(
            "PUT", self._lease_path(lease["metadata"]["name"]), json=lease)
        if response.status_code == 409:
            raise Conflict(lease["metadata"]["name"])
        response.raise_for_status()
        return response.json()

    def get_pod(self, name: str) -> dict[str, Any] | None:
        response = self._request("GET", self._pod_path(name))
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def label_pod(self, name: str, role: str) -> None:
        response = self._request(
            "PATCH",
            self._pod_path(name),
            json={"metadata": {"labels": {_ROLE_LABEL: role}}},
            headers={"Content-Type": "application/merge-patch+json"},
        )
        if response.status_code != 404:
            response.raise_for_status()

    def delete_pod(self, name: str, grace_seconds: int) -> None:
        response = self._request(
            "DELETE",
            self._pod_path(name),
            json={"gracePeriodSeconds": grace_seconds},
        )
        if response.status_code != 404:
            response.raise_for_status()

    def _get(self, path: str) -> dict[str, Any] | None:
        response = self._request("GET", path)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def statefulset_replicas(self, name: str) -> int | None:
        manifest = self._get(
            f"/apis/apps/v1/namespaces/{self.namespace}/statefulsets/{name}")
        return None if manifest is None else int(manifest["spec"].get("replicas", 1))

    def list_nodes(self) -> dict[str, dict[str, bool]]:
        """Node name -> whether it is Ready and accepts new database Pods."""
        nodes = {}
        for node in (self._get("/api/v1/nodes") or {}).get("items", []):
            ready = any(
                condition["type"] == "Ready" and condition["status"] == "True"
                for condition in node.get("status", {}).get("conditions", [])
            )
            blocked = node["spec"].get("unschedulable", False) or any(
                taint.get("effect") in {"NoSchedule", "NoExecute"}
                for taint in node["spec"].get("taints", [])
            )
            nodes[node["metadata"]["name"]] = {"ready": ready, "schedulable": ready and not blocked}
        return nodes

    def get_pvc(self, name: str) -> dict[str, Any] | None:
        return self._get(
            f"/api/v1/namespaces/{self.namespace}/persistentvolumeclaims/{name}")

    def delete_pvc(self, name: str) -> None:
        response = self._request(
            "DELETE", f"/api/v1/namespaces/{self.namespace}/persistentvolumeclaims/{name}")
        if response.status_code != 404:
            response.raise_for_status()

    def get_pv(self, name: str) -> dict[str, Any] | None:
        return self._get(f"/api/v1/persistentvolumes/{name}")

    def forget_pv(self, name: str) -> None:
        """Drop a volume object without asking its provisioner to wipe the disk.

        local-path cleans a volume with a helper Pod on the volume's node; on
        a dead node that Pod never runs and each attempt holds one of the
        provisioner's few workers for minutes, stalling every new volume.
        """
        path = f"/api/v1/persistentvolumes/{name}"
        response = self._request(
            "PATCH", path,
            json={"spec": {"persistentVolumeReclaimPolicy": "Retain"}},
            headers={"Content-Type": "application/merge-patch+json"},
        )
        if response.status_code == 404:
            return
        response.raise_for_status()
        response = self._request("DELETE", path)
        if response.status_code != 404:
            response.raise_for_status()


class LeaderElection:
    """Only the controller instance holding the ``db-controller`` Lease acts.

    Like client-go, expiry is judged by the follower's own clock: a holder is
    gone once its Lease has not changed for ``duration`` seconds. The holder
    stops acting after ``renew_deadline`` seconds without a successful renewal,
    which is shorter, so two instances never act at once.
    """

    def __init__(self, api: Any, identity: str, name: str = "db-controller",
                 duration: float = 10.0, renew_deadline: float = 6.0,
                 clock=time.monotonic) -> None:
        self.api = api
        self.identity = identity
        self.name = name
        self.duration = duration
        self.renew_deadline = renew_deadline
        self.clock = clock
        self.leading = threading.Event()
        self._observed_version: str | None = None
        self._observed_at = 0.0
        self._renewed_at: float | None = None

    def _attempt(self) -> bool | None:
        """True when this instance holds the Lease, False when another one
        does, None when the API could not be reached."""
        try:
            now = self.clock()
            lease = self.api.get_lease(self.name)
            if lease is None:
                self.api.create_lease(self.name, self.identity)
                self._renewed_at = now
                return True
            version = lease["metadata"].get("resourceVersion")
            if version != self._observed_version:
                self._observed_version, self._observed_at = version, now
            spec = lease["spec"]
            holder = spec.get("holderIdentity")
            if holder and holder != self.identity and now - self._observed_at < self.duration:
                return False
            if holder != self.identity:
                spec["leaseTransitions"] = spec.get("leaseTransitions", 0) + 1
                spec["acquireTime"] = _now()
                spec["holderIdentity"] = self.identity
            spec["renewTime"] = _now()
            spec["leaseDurationSeconds"] = int(self.duration)
            self.api.replace_lease(lease)
            self._renewed_at = now
            return True
        except Conflict:
            return False
        except httpx.HTTPError as error:
            _LOG.warning("controller_lease_unavailable error=%s", error)
            return None

    def step(self) -> bool:
        held = self._attempt()
        expired = self._renewed_at is None or self.clock() - self._renewed_at > self.renew_deadline
        if held or (held is None and not expired):
            if not self.leading.is_set():
                _LOG.warning("controller_leadership_acquired identity=%s", self.identity)
            self.leading.set()
        else:
            if self.leading.is_set():
                _LOG.warning("controller_leadership_lost identity=%s", self.identity)
            self.leading.clear()
        CONTROLLER_LEADING.set(1 if self.leading.is_set() else 0)
        return self.leading.is_set()

    def run(self, interval: float = 2.0) -> None:
        while True:
            self.step()
            time.sleep(interval)


@dataclass
class Probe:
    reachable: bool
    in_recovery: bool = False
    synchronous_standby_names: str = ""
    replicas: list[dict[str, Any]] = field(default_factory=list)
    timeline: int | None = None
    error: str | None = None


def _sync_names(setting: str) -> list[str]:
    """Standby names in synchronous_standby_names ('"a"', 'ANY 1 (a, b)', '*')."""
    value = setting.strip()
    if "(" in value:
        value = value[value.index("(") + 1:value.rindex(")")]
    return [name.strip().strip('"') for name in value.split(",") if name.strip()]


def _pv_node(volume: dict[str, Any]) -> str | None:
    """The node a node-local PersistentVolume lives on, if it is node-local."""
    terms = (volume["spec"].get("nodeAffinity", {}).get("required", {})
             .get("nodeSelectorTerms", []))
    for term in terms:
        for expression in term.get("matchExpressions", []):
            if expression.get("key") == _HOSTNAME_LABEL and expression.get("values"):
                return expression["values"][0]
    return None


class DatabaseController:
    def __init__(self, api: KubernetesApi, clusters: list[str]) -> None:
        self.api = api
        self.clusters = clusters
        self.user = os.environ["POSTGRES_USER"]
        self.password = os.environ["POSTGRES_PASSWORD"]
        self.failover_after = float(os.getenv("BANK_DB_FAILOVER_AFTER_SECONDS", "15"))
        self.interval = float(os.getenv("BANK_DB_CONTROLLER_INTERVAL_SECONDS", "2"))
        self.default_members = int(os.getenv("BANK_DB_MEMBERS", "2"))
        # "spread": keep a standby next to the primary and one on another
        # node, moving standbys when the layout is off (needs the matching
        # topology spread and Pod affinity in the StatefulSet).
        self.placement = os.getenv("BANK_DB_PLACEMENT", "")
        self.stray_after = float(os.getenv("BANK_DB_STRAY_STANDBY_SECONDS", "30"))
        self.rebalance_every = float(os.getenv("BANK_DB_REBALANCE_INTERVAL_SECONDS", "180"))
        self.rebuild_unreachable_after = float(
            os.getenv("BANK_DB_REBUILD_UNREACHABLE_AFTER_SECONDS", "45"))
        self.degraded_since: dict[str, float | None] = {cluster: None for cluster in clusters}
        self.leader_down_since: dict[str, float | None] = {
            cluster: None for cluster in clusters}
        self.stray_since: dict[str, dict[str, float]] = {cluster: {} for cluster in clusters}
        self.last_rebalance: dict[str, float] = {cluster: 0.0 for cluster in clusters}
        self.last_storage_check: dict[str, float] = {cluster: 0.0 for cluster in clusters}
        self.pod_ips: dict[str, str] = {}

    def host(self, cluster: str, pod: str) -> str:
        # The Pod IP keeps failover independent of cluster DNS, which half
        # fails while a dead node's CoreDNS replica is still an endpoint.
        return self.pod_ips.get(pod) or f"{pod}.{cluster}.{self.api.namespace}.svc.cluster.local"

    def members(self, cluster: str) -> list[str]:
        try:
            replicas = self.api.statefulset_replicas(cluster)
        except httpx.HTTPError:
            replicas = None
        count = replicas if replicas is not None else self.default_members
        return [f"{cluster}-{index}" for index in range(max(count, 1))]

    def _connect(self, cluster: str, pod: str):
        connection = psycopg2.connect(
            host=self.host(cluster, pod),
            port=5432,
            user=self.user,
            password=self.password,
            dbname="postgres",
            connect_timeout=2,
            application_name="db-controller",
            options="-c statement_timeout=5000",
        )
        connection.autocommit = True
        return connection

    def probe(self, cluster: str, pod: str) -> Probe:
        try:
            connection = self._connect(cluster, pod)
        except psycopg2.Error as error:
            return Probe(False, error=str(error).strip().splitlines()[0])
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_is_in_recovery(), "
                    "current_setting('synchronous_standby_names')"
                )
                in_recovery, synchronous_names = cursor.fetchone()
                replicas: list[dict[str, Any]] = []
                timeline = None
                if not in_recovery:
                    cursor.execute(
                        "SELECT ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))"
                        "::bit(32)::int"
                    )
                    timeline = int(cursor.fetchone()[0])
                    cursor.execute(
                        "SELECT application_name, state, sync_state, "
                        "COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn), -1) "
                        "FROM pg_stat_replication"
                    )
                    replicas = [
                        {
                            "name": name,
                            "state": state,
                            "sync_state": sync_state,
                            "lag_bytes": int(lag),
                        }
                        for name, state, sync_state, lag in cursor.fetchall()
                    ]
            return Probe(True, bool(in_recovery), synchronous_names or "", replicas, timeline)
        except psycopg2.Error as error:
            return Probe(False, error=str(error).strip().splitlines()[0])
        finally:
            connection.close()

    def promote(self, cluster: str, pod: str) -> bool:
        try:
            connection = self._connect(cluster, pod)
        except psycopg2.Error:
            return False
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_promote(true, 60)")
                promoted = bool(cursor.fetchone()[0])
        except psycopg2.Error as error:
            _LOG.warning("promote_failed cluster=%s pod=%s error=%s", cluster, pod, error)
            return False
        finally:
            connection.close()
        _LOG.warning("promoted cluster=%s pod=%s result=%s", cluster, pod, promoted)
        return promoted

    def set_synchronous_standby(self, cluster: str, leader: str, standby: str) -> bool:
        try:
            connection = self._connect(cluster, leader)
        except psycopg2.Error:
            return False
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "ALTER SYSTEM SET synchronous_standby_names = %s", (f'"{standby}"',))
                cursor.execute("SELECT pg_reload_conf()")
        except psycopg2.Error as error:
            _LOG.warning("set_sync_standby_failed cluster=%s error=%s", cluster, error)
            return False
        finally:
            connection.close()
        _LOG.warning("synchronous_standby_configured cluster=%s leader=%s standby=%s",
                     cluster, leader, standby)
        return True

    def _update_lease(self, lease: dict[str, Any], **annotations: str | None) -> dict[str, Any]:
        current = lease["metadata"].setdefault("annotations", {})
        for key, value in annotations.items():
            if value is None:
                current.pop(key, None)
            else:
                current[key] = value
        return self.api.replace_lease(lease)

    def _fence(self, cluster: str, pod: str, epoch: int, reason: str) -> None:
        _LOG.error("fencing cluster=%s pod=%s reason=%s epoch=%s", cluster, pod, reason, epoch)
        self.api.label_pod(pod, "replica")
        self.api.delete_pod(pod, 0)
        FENCES.labels(cluster).inc()

    def reconcile(self, cluster: str) -> None:
        lease = self.api.get_lease(f"{cluster}-leader")
        if lease is None:
            return
        leader = lease["spec"]["holderIdentity"]
        epoch = int(lease["spec"].get("leaseTransitions", 0))
        annotations = lease["metadata"].get("annotations", {})
        members = self.members(cluster)
        if leader not in members:
            members.append(leader)
        pods = {pod: self.api.get_pod(pod) for pod in members}
        for pod, manifest in pods.items():
            ip = (manifest or {}).get("status", {}).get("podIP")
            if ip:
                self.pod_ips[pod] = ip
            else:
                self.pod_ips.pop(pod, None)
        probes = {pod: self.probe(cluster, pod) for pod in members}
        nodes = self.api.list_nodes()
        node_of = {
            pod: (manifest or {}).get("spec", {}).get("nodeName") for pod, manifest in pods.items()
        }
        leader_probe = probes[leader]
        leader_is_primary = leader_probe.reachable and not leader_probe.in_recovery
        replicas = {item["name"]: item for item in leader_probe.replicas} if leader_is_primary else {}
        synced = annotations.get(_ANNOTATION_SYNCED_STANDBY)

        LEADER_EPOCH.labels(cluster).set(epoch)
        for pod, probe in probes.items():
            POD_UP.labels(cluster, pod).set(1 if probe.reachable else 0)
            POD_PRIMARY.labels(cluster, pod).set(
                1 if probe.reachable and not probe.in_recovery else 0)
        for name, replica in replicas.items():
            REPLICATION_LAG.labels(cluster, name).set(replica["lag_bytes"])
        sync_streaming = bool(
            synced and synced in replicas
            and replicas[synced]["state"] == "streaming"
            and replicas[synced]["sync_state"] in {"sync", "quorum"}
        )
        SYNC_STANDBY.labels(cluster).set(1 if sync_streaming else 0)
        WRITABLE.labels(cluster).set(1 if leader_is_primary and sync_streaming else 0)

        self.recover_lost_storage(cluster, members, leader, pods, nodes)

        # Any primary that is not the recorded leader is either an old primary
        # (fence it) or proof that the record itself is stale (adopt it).
        rivals = [
            pod for pod in members
            if pod != leader and probes[pod].reachable and not probes[pod].in_recovery
        ]
        if rivals:
            newest = max(rivals, key=lambda pod: probes[pod].timeline or -1)
            recorded = annotations.get(_ANNOTATION_TIMELINE)
            newest_timeline = probes[newest].timeline or -1
            if (
                recorded is not None
                and newest_timeline > int(recorded)
                and not (leader_is_primary and (leader_probe.timeline or -1) >= newest_timeline)
            ):
                self.adopt(cluster, lease, newest, newest_timeline)
                return
            for pod in rivals:
                self._fence(cluster, pod, epoch, "non_leader_running_as_primary")
        for pod in members:
            if pod != leader and pod not in rivals and pods[pod] is not None and (
                    pods[pod]["metadata"].get("labels", {}).get(_ROLE_LABEL) != "replica"):
                self.api.label_pod(pod, "replica")

        if leader_probe.reachable:
            self.leader_down_since[cluster] = None
            if leader_probe.in_recovery:
                if annotations.get(_ANNOTATION_PROMOTING) == "true":
                    self.promote(cluster, leader)
                else:
                    _LOG.error(
                        "leader_is_standby cluster=%s leader=%s epoch=%s "
                        "reason=leader_record_older_than_database_state action=wait",
                        cluster, leader, epoch)
                return
            self.manage_primary(cluster, lease, leader, members, probes, pods, node_of, nodes)
            return

        now = time.monotonic()
        down_since = self.leader_down_since[cluster]
        if down_since is None:
            self.leader_down_since[cluster] = now
            _LOG.warning("leader_unreachable cluster=%s leader=%s error=%s",
                         cluster, leader, leader_probe.error)
            return
        if now - down_since < self.failover_after:
            return
        if not synced:
            _LOG.error(
                "failover_blocked cluster=%s leader=%s reason=no_recorded_synchronous_standby "
                "hint='manual failover with --force accepts possible loss of acknowledged commits'",
                cluster, leader)
            return
        candidate = probes.get(synced)
        if candidate is None or not (candidate.reachable and candidate.in_recovery):
            _LOG.error(
                "failover_blocked cluster=%s leader=%s reason=synchronous_standby_unavailable "
                "standby=%s", cluster, leader, synced)
            return
        self.failover(cluster, reason="leader_unreachable", lease=lease)

    def manage_primary(
        self,
        cluster: str,
        lease: dict[str, Any],
        leader: str,
        members: list[str],
        probes: dict[str, Probe],
        pods: dict[str, dict[str, Any] | None],
        node_of: dict[str, str | None],
        nodes: dict[str, dict[str, bool]],
    ) -> None:
        annotations = lease["metadata"].get("annotations", {})
        epoch = int(lease["spec"].get("leaseTransitions", 0))
        leader_probe = probes[leader]
        replicas = {item["name"]: item for item in leader_probe.replicas}
        if (pods[leader] or {}).get("metadata", {}).get("labels", {}).get(_ROLE_LABEL) != "primary":
            self.api.label_pod(leader, "primary")
            _LOG.info("routing cluster=%s primary=%s epoch=%s", cluster, leader, epoch)

        updates: dict[str, str | None] = {}
        if annotations.get(_ANNOTATION_INITIALIZED) != "true":
            updates[_ANNOTATION_INITIALIZED] = "true"
        if annotations.get(_ANNOTATION_TIMELINE) != str(leader_probe.timeline):
            updates[_ANNOTATION_TIMELINE] = str(leader_probe.timeline)
        if annotations.get(_ANNOTATION_PROMOTING):
            updates[_ANNOTATION_PROMOTING] = None

        def remote(pod: str) -> bool:
            return bool(node_of.get(pod) and node_of.get(leader)
                        and node_of[pod] != node_of[leader]
                        and nodes.get(node_of[pod], {}).get("ready", False))

        streaming = [
            pod for pod in members
            if pod != leader and replicas.get(pod, {}).get("state") == "streaming"
        ]
        configured = _sync_names(leader_probe.synchronous_standby_names)
        current = configured[0] if len(configured) == 1 else None
        current_ok = bool(
            current and current in streaming
            and replicas[current]["sync_state"] in {"sync", "quorum"}
        )
        ranked = sorted(streaming, key=lambda pod: (not remote(pod), pod != current, pod))
        preferred = ranked[0] if ranked else None
        switch_to = None
        if current_ok:
            if preferred and preferred != current and remote(preferred) and not remote(current):
                switch_to = preferred
            elif annotations.get(_ANNOTATION_SYNCED_STANDBY) != current:
                updates[_ANNOTATION_SYNCED_STANDBY] = current
                _LOG.info("standby_synchronized cluster=%s standby=%s remote=%s epoch=%s",
                          cluster, current, remote(current), epoch)
        elif preferred and configured != [preferred]:
            switch_to = preferred
        elif not streaming:
            _LOG.warning("degraded cluster=%s reason=no_streaming_standby commits=blocked",
                         cluster)
            self.replace_unreachable_standby(cluster, leader, members, probes, node_of, nodes)
        if streaming:
            self.degraded_since[cluster] = None
        if switch_to:
            # Forget the old synchronous standby before PostgreSQL stops
            # waiting for it: from here until the new one is reported as
            # sync there is no automatic failover target.
            updates[_ANNOTATION_SYNCED_STANDBY] = None
        if updates:
            try:
                lease = self._update_lease(lease, **updates)
            except Conflict:
                return
        if switch_to:
            self.set_synchronous_standby(cluster, leader, switch_to)
            return

        self.restart_stray_standbys(cluster, leader, members, probes, replicas)
        if self.placement == "spread" and current_ok and len(streaming) == len(members) - 1:
            self.rebalance(cluster, leader, members, current, node_of, nodes)

    def restart_stray_standbys(
        self,
        cluster: str,
        leader: str,
        members: list[str],
        probes: dict[str, Probe],
        replicas: dict[str, dict[str, Any]],
    ) -> None:
        """Standbys still following an old leader re-bootstrap from the new one."""
        now = time.monotonic()
        stray = self.stray_since[cluster]
        for pod in members:
            if pod == leader or not (probes[pod].reachable and probes[pod].in_recovery) \
                    or pod in replicas:
                stray.pop(pod, None)
                continue
            since = stray.setdefault(pod, now)
            if now - since >= self.stray_after:
                _LOG.warning("restarting_stray_standby cluster=%s pod=%s leader=%s",
                             cluster, pod, leader)
                stray.pop(pod, None)
                self.api.delete_pod(pod, 10)

    def rebalance(
        self,
        cluster: str,
        leader: str,
        members: list[str],
        synchronous: str | None,
        node_of: dict[str, str | None],
        nodes: dict[str, dict[str, bool]],
    ) -> None:
        if time.monotonic() - self.last_rebalance[cluster] < self.rebalance_every:
            return
        leader_node = node_of.get(leader)
        standbys = [pod for pod in members if pod != leader]
        local = [pod for pod in standbys if node_of.get(pod) == leader_node]
        remote = [pod for pod in standbys if node_of.get(pod) != leader_node]
        movable = [pod for pod in standbys if pod != synchronous]
        free_nodes = [
            name for name, info in nodes.items()
            if info["schedulable"] and name != leader_node
        ]
        victim = None
        if not remote and free_nodes and movable:
            victim, reason = movable[-1], "no_standby_on_another_node"
        elif not local and len(remote) >= 2:
            candidates = [pod for pod in remote if pod in movable]
            if candidates:
                victim, reason = candidates[-1], "no_standby_next_to_primary"
        if victim is None:
            return
        self.last_rebalance[cluster] = time.monotonic()
        _LOG.warning("rebalancing cluster=%s pod=%s node=%s reason=%s",
                     cluster, victim, node_of.get(victim), reason)
        self.rebuild(cluster, victim, reason, grace_seconds=10)

    def replace_unreachable_standby(
        self,
        cluster: str,
        leader: str,
        members: list[str],
        probes: dict[str, Probe],
        node_of: dict[str, str | None],
        nodes: dict[str, dict[str, bool]],
    ) -> None:
        """Commits block while the primary has no standby. When that lasts and
        a standby's node is NotReady, re-clone that standby on a healthy node.

        Its old copy holds nothing that is not on the primary (every
        acknowledged commit reached the synchronous standby that is now the
        primary), so discarding it trades only resync time for availability.
        """
        if self.rebuild_unreachable_after <= 0:
            return
        now = time.monotonic()
        since = self.degraded_since[cluster]
        if since is None:
            self.degraded_since[cluster] = now
            return
        if now - since < self.rebuild_unreachable_after:
            return
        healthy = [name for name, info in nodes.items() if info["ready"] and info["schedulable"]]
        lost: dict[str, str] = {}
        for pod in members:
            if pod == leader:
                continue
            # A Pending Pod is pinned to wherever its volume is.
            where = node_of.get(pod) or self._volume_node(pod)
            if where is None or nodes.get(where, {}).get("ready"):
                return          # a standby is (re)starting on a healthy node
            if not probes[pod].reachable:
                lost[pod] = where
        if not healthy or not lost:
            return
        pod = sorted(lost)[0]
        self.degraded_since[cluster] = now
        _LOG.error("standby_unreachable cluster=%s pod=%s node=%s action=rebuild_on_healthy_node",
                   cluster, pod, lost[pod])
        self.rebuild(cluster, pod, "node_unreachable", grace_seconds=0, node_lost=True)

    def _volume_node(self, pod: str) -> str | None:
        claim = self.api.get_pvc(f"data-{pod}")
        volume_name = (claim or {}).get("spec", {}).get("volumeName")
        volume = self.api.get_pv(volume_name) if volume_name else None
        return _pv_node(volume) if volume else None

    def rebuild(self, cluster: str, pod: str, reason: str, grace_seconds: int,
                node_lost: bool = False) -> None:
        """Discard a standby's volume; the StatefulSet recreates and re-clones it.

        ``node_lost``: the volume's node cannot run the provisioner's cleanup,
        so only the volume object is removed.
        """
        claim = self.api.get_pvc(f"data-{pod}") if node_lost else None
        self.api.delete_pvc(f"data-{pod}")
        volume_name = (claim or {}).get("spec", {}).get("volumeName")
        if volume_name:
            self.api.forget_pv(volume_name)
        self.api.delete_pod(pod, grace_seconds)
        REBUILDS.labels(cluster, reason).inc()

    def recover_lost_storage(
        self,
        cluster: str,
        members: list[str],
        leader: str,
        pods: dict[str, dict[str, Any] | None],
        nodes: dict[str, dict[str, bool]],
    ) -> None:
        """Rebuild standbys whose node (and node-local volume) was removed for good.

        Deleting the Node object declares that the machine and its disks are
        gone. (A NotReady node may come back; replace_unreachable_standby
        covers that case only while the primary has no standby at all.)
        """
        now = time.monotonic()
        if not nodes or now - self.last_storage_check[cluster] < 10:
            return
        self.last_storage_check[cluster] = now
        for pod in members:
            if pod == leader:
                continue
            claim = self.api.get_pvc(f"data-{pod}")
            volume_name = (claim or {}).get("spec", {}).get("volumeName")
            volume = self.api.get_pv(volume_name) if volume_name else None
            volume_node = _pv_node(volume) if volume else None
            if volume_node and volume_node not in nodes:
                _LOG.error("member_storage_lost cluster=%s pod=%s node=%s action=rebuild",
                           cluster, pod, volume_node)
                self.rebuild(cluster, pod, "node_removed", grace_seconds=0, node_lost=True)
                continue
            pod_node = (pods.get(pod) or {}).get("spec", {}).get("nodeName")
            if pod_node and pod_node not in nodes:
                _LOG.warning("pod_on_removed_node cluster=%s pod=%s node=%s",
                             cluster, pod, pod_node)
                self.api.delete_pod(pod, 0)

    def adopt(self, cluster: str, lease: dict[str, Any], pod: str, timeline: int) -> None:
        """Point a stale leader record at the primary that is demonstrably newer."""
        old_leader = lease["spec"]["holderIdentity"]
        lease["spec"]["holderIdentity"] = pod
        lease["spec"]["leaseTransitions"] = int(lease["spec"].get("leaseTransitions", 0)) + 1
        lease["spec"]["acquireTime"] = _now()
        annotations = lease["metadata"].setdefault("annotations", {})
        annotations.pop(_ANNOTATION_SYNCED_STANDBY, None)
        annotations.pop(_ANNOTATION_PROMOTING, None)
        annotations[_ANNOTATION_TIMELINE] = str(timeline)
        try:
            lease = self.api.replace_lease(lease)
        except Conflict:
            return
        self.api.label_pod(old_leader, "replica")
        self.api.label_pod(pod, "primary")
        self.leader_down_since[cluster] = None
        FAILOVERS.labels(cluster, "stale_leader_record").inc()
        _LOG.warning(
            "leader_record_adopted cluster=%s old_leader=%s leader=%s timeline=%s epoch=%s",
            cluster, old_leader, pod, timeline, lease["spec"]["leaseTransitions"])

    def failover(
        self,
        cluster: str,
        reason: str,
        lease: dict[str, Any] | None = None,
        force: bool = False,
        target: str | None = None,
    ) -> str:
        lease = lease or self.api.get_lease(f"{cluster}-leader")
        if lease is None:
            raise RuntimeError(f"{cluster} has no leader record yet.")
        old_leader = lease["spec"]["holderIdentity"]
        annotations = lease["metadata"].get("annotations", {})
        synced = annotations.get(_ANNOTATION_SYNCED_STANDBY)
        new_leader = target or synced
        if not new_leader:
            raise RuntimeError(
                f"{cluster} has no recorded synchronous standby; name one with --to "
                "and --force only if losing acknowledged commits is acceptable.")
        if new_leader == old_leader:
            raise RuntimeError(f"{new_leader} is already the leader.")
        candidate = self.probe(cluster, new_leader)
        if not (candidate.reachable and candidate.in_recovery):
            raise RuntimeError(f"{new_leader} is not a running standby; cannot promote it.")
        if new_leader != synced and not force:
            raise RuntimeError(
                f"{new_leader} is not the recorded synchronous standby of {old_leader}; "
                "use --force only if losing acknowledged commits is acceptable.")
        old_probe = self.probe(cluster, old_leader)

        # 1. Compare-and-swap the leader record: exactly one failover wins.
        lease["spec"]["holderIdentity"] = new_leader
        lease["spec"]["leaseTransitions"] = int(lease["spec"].get("leaseTransitions", 0)) + 1
        lease["spec"]["acquireTime"] = _now()
        lease["metadata"].setdefault("annotations", {}).pop(_ANNOTATION_SYNCED_STANDBY, None)
        lease["metadata"]["annotations"][_ANNOTATION_PROMOTING] = "true"
        lease = self.api.replace_lease(lease)
        epoch = lease["spec"]["leaseTransitions"]
        _LOG.warning(
            "failover_started cluster=%s old_leader=%s new_leader=%s epoch=%s reason=%s",
            cluster, old_leader, new_leader, epoch, reason)

        # 2. Fence the old primary before the new one becomes writable. A
        # healthy old primary gets a fast shutdown so it streams its last WAL.
        self.api.label_pod(old_leader, "replica")
        old_pod = self.api.get_pod(old_leader)
        old_uid = old_pod["metadata"]["uid"] if old_pod else None
        self.api.delete_pod(old_leader, 20 if old_probe.reachable else 0)
        deadline = time.monotonic() + 40
        while old_uid is not None and time.monotonic() < deadline:
            current = self.api.get_pod(old_leader)
            if current is None or current["metadata"]["uid"] != old_uid:
                break
            time.sleep(1)

        # 3. Promote. If this fails, the reconcile loop keeps promoting the
        # recorded leader (the promoting mark is set) until it succeeds.
        self.promote(cluster, new_leader)
        self.api.label_pod(new_leader, "primary")
        FAILOVERS.labels(cluster, reason).inc()
        self.leader_down_since[cluster] = None

        # 4. Other standbys still stream from the old leader: restart them so
        # they re-bootstrap from the new one.
        for pod in self.members(cluster):
            if pod not in {old_leader, new_leader}:
                self.api.delete_pod(pod, 10)
        _LOG.warning("failover_completed cluster=%s leader=%s epoch=%s",
                     cluster, new_leader, epoch)
        return new_leader

    def cluster_status(self, cluster: str) -> dict[str, Any]:
        lease = self.api.get_lease(f"{cluster}-leader")
        members = self.members(cluster)
        probes = {pod: self.probe(cluster, pod) for pod in members}
        nodes = {}
        for pod in members:
            manifest = self.api.get_pod(pod)
            nodes[pod] = (manifest or {}).get("spec", {}).get("nodeName")
        return {
            "cluster": cluster,
            "leader": lease["spec"]["holderIdentity"] if lease else None,
            "epoch": lease["spec"].get("leaseTransitions") if lease else None,
            "annotations": lease["metadata"].get("annotations", {}) if lease else {},
            "pods": {
                pod: {
                    "node": nodes[pod],
                    "reachable": probe.reachable,
                    "role": (
                        None if not probe.reachable
                        else "standby" if probe.in_recovery else "primary"
                    ),
                    "timeline": probe.timeline,
                    "synchronous_standby_names": probe.synchronous_standby_names or None,
                    "replicas": probe.replicas,
                    "error": probe.error,
                }
                for pod, probe in probes.items()
            },
        }

    def run_cluster(self, cluster: str, leading: threading.Event | None = None) -> None:
        while True:
            if leading is not None and not leading.is_set():
                self.leader_down_since[cluster] = None
                self.degraded_since[cluster] = None
                self.stray_since[cluster].clear()
                leading.wait(self.interval)
                continue
            try:
                self.reconcile(cluster)
            except Exception:
                _LOG.exception("reconcile_failed cluster=%s", cluster)
            time.sleep(self.interval)


def bootstrap(api: KubernetesApi, cluster: str, pod: str, output: Path) -> None:
    lease_name = f"{cluster}-leader"
    while True:
        try:
            lease = api.get_lease(lease_name)
            if lease is None:
                try:
                    lease = api.create_lease(lease_name, f"{cluster}-0")
                    _LOG.info("leader_record_created cluster=%s leader=%s-0", cluster, cluster)
                except Conflict:
                    continue
            break
        except httpx.HTTPError as error:
            _LOG.warning("kubernetes_api_unavailable error=%s", error)
            time.sleep(2)
    leader = lease["spec"]["holderIdentity"]
    initialized = lease["metadata"].get("annotations", {}).get(
        _ANNOTATION_INITIALIZED, "false")
    output.mkdir(parents=True, exist_ok=True)
    (output / "role").write_text("primary" if leader == pod else "standby")
    (output / "leader").write_text(leader)
    (output / "epoch").write_text(str(lease["spec"].get("leaseTransitions", 0)))
    (output / "initialized").write_text(initialized)
    _LOG.info(
        "bootstrap cluster=%s pod=%s role=%s leader=%s epoch=%s initialized=%s",
        cluster, pod, "primary" if leader == pod else "standby", leader,
        lease["spec"].get("leaseTransitions", 0), initialized)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.getenv("BANK_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    boot = commands.add_parser("bootstrap")
    boot.add_argument("--cluster", required=True)
    boot.add_argument("--pod", required=True)
    boot.add_argument("--output", type=Path, default=Path("/bootstrap"))
    commands.add_parser("run")
    manual = commands.add_parser("failover")
    manual.add_argument("cluster")
    manual.add_argument("--to", help="standby to promote (default: the synchronous standby)")
    manual.add_argument("--force", action="store_true")
    commands.add_parser("status")
    args = parser.parse_args(argv)

    api = KubernetesApi()
    if args.command == "bootstrap":
        bootstrap(api, args.cluster, args.pod, args.output)
        return 0

    clusters = [
        value.strip()
        for value in os.getenv(
            "BANK_DB_CLUSTERS", "shard-a-db,shard-b-db,coordinator-db").split(",")
        if value.strip()
    ]
    controller = DatabaseController(api, clusters)
    if args.command == "status":
        print(json.dumps([controller.cluster_status(c) for c in clusters], indent=2))
        return 0
    if args.command == "failover":
        try:
            leader = controller.failover(
                args.cluster, "manual", force=args.force, target=args.to)
        except (RuntimeError, Conflict) as error:
            print(f"Failover refused: {error}", file=sys.stderr)
            return 1
        print(f"{args.cluster}: {leader} is now the primary.")
        return 0

    start_http_server(int(os.getenv("BANK_METRICS_PORT", "9100")))
    election = LeaderElection(
        api, os.getenv("POD_NAME") or os.uname().nodename,
        duration=float(os.getenv("BANK_DB_CONTROLLER_LEASE_SECONDS", "10")),
        renew_deadline=float(os.getenv("BANK_DB_CONTROLLER_RENEW_DEADLINE_SECONDS", "6")))
    threads = [threading.Thread(target=election.run, daemon=True)] + [
        threading.Thread(target=controller.run_cluster, args=(cluster, election.leading), daemon=True)
        for cluster in clusters
    ]
    for thread in threads:
        thread.start()
    _LOG.info("db_controller_started clusters=%s", ",".join(clusters))
    for thread in threads:
        thread.join()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
