"""VM failure experiments on the three-VM cluster (bonus I and II).

    python scripts/k8s_vm_experiments.py worker-interrupt      # bonus I
    python scripts/k8s_vm_experiments.py worker-destroy        # bonus II, scenario A
    python scripts/k8s_vm_experiments.py control-plane-loss    # bonus II, scenario B

Each experiment runs the same closed-loop load as scripts/k8s_experiments.py
through the edge proxy (http://127.0.0.1:18080) and ends with the same checks
against the databases (ledger per request ID, money conservation, 2PC states
on coordinator and participants). In addition, a sampler asks every database
member every few seconds whether it is a writable primary, so "never two
writable databases" is measured rather than assumed.
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import k8s_experiments as experiments  # noqa: E402
from k8s_multivm import Cluster  # noqa: E402

CLUSTERS = ("shard-a-db", "shard-b-db", "coordinator-db")


def log(message: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {message}", flush=True)


class WritableSampler(threading.Thread):
    """Counts, per database cluster, the members that answer as primaries."""

    def __init__(self, k8s: experiments.Kubernetes, avoid_node: str | None = None) -> None:
        super().__init__(daemon=True)
        self.k8s = k8s
        self.avoid_node = avoid_node
        self.samples: list[dict[str, Any]] = []
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()
        self.join(timeout=60)

    def sample_cluster(self, cluster: str) -> dict[str, str] | None:
        pods = [
            pod for pod in self.k8s.pods(cluster)
            if pod.get("status", {}).get("phase") == "Running"
            and not pod["metadata"].get("deletionTimestamp")
            and pod["spec"].get("nodeName") != self.avoid_node
        ]
        if not pods:
            return None
        members = sorted(pod["metadata"]["name"] for pod in self.k8s.pods(cluster))
        script = "; ".join(
            f"printf '{member}='; PGCONNECT_TIMEOUT=2 PGPASSWORD=$POSTGRES_PASSWORD "
            f"psql -h {member}.{cluster} -U $POSTGRES_USER -d postgres -Atc "
            f"'SELECT NOT pg_is_in_recovery()' 2>/dev/null || echo down"
            for member in members
        )
        output = self.k8s.run("exec", pods[0]["metadata"]["name"], "-c", "postgres", "--",
                              "bash", "-c", script, check=False, timeout=30)
        result = {}
        for line in output.splitlines():
            if "=" in line:
                member, answer = line.split("=", 1)
                result[member] = {"t": "primary", "f": "standby"}.get(answer.strip(), "down")
        return result

    def run(self) -> None:
        started = time.monotonic()
        while not self._stop.is_set():
            sample: dict[str, Any] = {"t": round(time.monotonic() - started, 1)}
            for cluster in CLUSTERS:
                try:
                    sample[cluster] = self.sample_cluster(cluster)
                except Exception as error:  # noqa: BLE001 - keep sampling
                    sample[cluster] = {"error": str(error)[:120]}
            self.samples.append(sample)
            self._stop.wait(2)

    def summary(self) -> dict[str, Any]:
        worst = {}
        for cluster in CLUSTERS:
            counts = [
                sum(1 for state in sample[cluster].values() if state == "primary")
                for sample in self.samples
                if isinstance(sample.get(cluster), dict) and "error" not in sample[cluster]
            ]
            worst[cluster] = {
                "samples": len(counts),
                "max_writable_primaries": max(counts, default=None),
                "samples_without_primary": sum(1 for count in counts if count == 0),
            }
        return {
            "per_cluster": worst,
            "never_two_writable": all(
                (item["max_writable_primaries"] or 0) <= 1 for item in worst.values()),
        }


class VmExperiments(experiments.Experiments):
    def __init__(self, args: argparse.Namespace) -> None:
        super().__init__(args)
        self.cluster = Cluster(args.profile)

    # --------------------------------------------------------------- helpers
    def db_status(self) -> dict[str, Any]:
        return self.k8s.database_status()

    def primaries_by_node(self) -> dict[str, list[str]]:
        result: dict[str, list[str]] = {}
        for cluster, status in self.db_status().items():
            node = status["pods"][status["leader"]]["node"]
            result.setdefault(node, []).append(f"{cluster}:{status['leader']}")
        return result

    def fully_redundant(self, require_remote: bool = True) -> bool:
        """Every cluster: primary, all members streaming, synchronous standby recorded
        (and, when required, on another worker than the primary)."""
        try:
            status = self.db_status()
        except Exception:  # noqa: BLE001
            return False
        for cluster in CLUSTERS:
            entry = status.get(cluster)
            if not entry or not self.k8s.replication_healthy(cluster):
                return False
            synced = entry["annotations"].get("banking.local/synced-standby")
            if not synced:
                return False
            if require_remote and entry["pods"][synced]["node"] == entry["pods"][entry["leader"]]["node"]:
                return False
        return True

    def wait_until(self, condition, timeout: float, what: str, interval: float = 5) -> float:
        started = time.monotonic()
        while time.monotonic() - started < timeout:
            try:
                if condition():
                    return round(time.monotonic() - started, 1)
            except Exception:  # noqa: BLE001 - the API may be briefly unavailable
                pass
            time.sleep(interval)
        raise TimeoutError(f"timed out after {timeout:.0f} s waiting for {what}")

    def layout_snapshot(self) -> dict[str, Any]:
        status = self.db_status()
        return {
            cluster: {
                "leader": entry["leader"],
                "epoch": entry["epoch"],
                "synchronous_standby": entry["annotations"].get("banking.local/synced-standby"),
                "members": {name: f"{pod['role']}@{pod['node']}" for name, pod in entry["pods"].items()},
            }
            for cluster, entry in status.items()
        }

    def app_pods_by_node(self) -> dict[str, dict[str, int]]:
        result: dict[str, dict[str, int]] = {}
        for name in experiments.DEPLOYMENTS:
            for pod in self.k8s.pods(name):
                if self.k8s.is_ready(pod):
                    node = pod["spec"]["nodeName"]
                    result.setdefault(node, {}).setdefault(name, 0)
                    result[node][name] += 1
        return result

    def spread_apps(self) -> None:
        """Kubernetes never moves running Pods, so after a worker outage every
        app replica stays on the survivor; a rolling restart spreads them again."""
        workers = set(self.cluster.workers())
        for name in experiments.DEPLOYMENTS:
            nodes = {pod["spec"].get("nodeName") for pod in self.k8s.pods(name)
                     if self.k8s.is_ready(pod)}
            if not workers <= nodes:
                log(f"{name} runs only on {sorted(nodes)}; restarting it to spread its replicas")
                self.k8s.run("rollout", "restart", f"deployment/{name}")
                self.k8s.run("rollout", "status", f"deployment/{name}", "--timeout=300s", timeout=320)

    def in_flight_transfers(self) -> list[dict[str, str]]:
        rows = self.k8s.psql("coordinator-db", (
            "SELECT t.transaction_id, t.state, coalesce(host(a.client_addr), '') "
            "FROM bank_coordinated_transfers t "
            "LEFT JOIN pg_locks l ON l.locktype = 'advisory' AND l.granted AND l.objsubid = 1 "
            "AND l.classid::bigint = ((hashtextextended('bank-2pc:' || t.transaction_id, 0) >> 32) & 4294967295) "
            "AND l.objid::bigint = (hashtextextended('bank-2pc:' || t.transaction_id, 0) & 4294967295) "
            "LEFT JOIN pg_stat_activity a ON a.pid = l.pid "
            "WHERE t.state IN ('PREPARING', 'COMMIT', 'ABORT')"
        ))
        owners = {
            pod.get("status", {}).get("podIP"): (pod["metadata"]["name"], pod["spec"].get("nodeName"))
            for pod in self.k8s.pods("gateway")
        }
        return [
            {"transaction_id": tid, "state": state,
             "owner": owners.get(ip, (ip or None, None))[0],
             "owner_node": owners.get(ip, (None, None))[1]}
            for tid, state, ip in rows
        ]

    def transfer_outcomes(self, ids: list[str]) -> dict[str, Any]:
        if not ids:
            return {}
        quoted = ",".join(f"'{tid}'" for tid in ids)
        coordinator = dict(self.k8s.psql("coordinator-db", (
            f"SELECT transaction_id, state FROM bank_coordinated_transfers "
            f"WHERE transaction_id IN ({quoted})")))
        participants: dict[str, dict[str, str]] = {}
        for cluster in ("shard-a-db", "shard-b-db"):
            for tid, role, state in self.k8s.psql(cluster, (
                f"SELECT transaction_id, role, state FROM bank_prepared_transfers "
                f"WHERE transaction_id IN ({quoted})")):
                participants.setdefault(tid, {})[role] = state
        return {
            tid: {"coordinator": coordinator.get(tid), "participants": participants.get(tid, {})}
            for tid in ids
        }

    def set_failpoints(self, seconds: float) -> None:
        if seconds:
            self.k8s.run("set", "env", "deployment/gateway",
                         f"BANK_EXPERIMENT_PAUSE_AFTER_PREPARE_SECONDS={seconds}",
                         f"BANK_EXPERIMENT_PAUSE_AFTER_COMMIT_SECONDS={seconds}")
        else:
            self.k8s.run("set", "env", "deployment/gateway",
                         "BANK_EXPERIMENT_PAUSE_AFTER_PREPARE_SECONDS-",
                         "BANK_EXPERIMENT_PAUSE_AFTER_COMMIT_SECONDS-")
        self.k8s.run("rollout", "status", "deployment/gateway", "--timeout=300s", timeout=320)

    def pick_worker(self, requested: str | None) -> str:
        if requested:
            return requested
        by_node = self.primaries_by_node()
        return max(self.cluster.workers(), key=lambda node: len(by_node.get(node, [])))

    # ------------------------------------------------------------ scenarios
    def scenario_worker_interrupt(self, node: str | None, outage: float) -> dict[str, Any]:
        """Bonus I: power-cut one worker under load, recover, power it back on."""
        self.wait_until(self.fully_redundant, 600, "a fully redundant starting layout")
        self.spread_apps()
        victim = self.pick_worker(node)
        before = self.layout_snapshot()
        primaries_on_victim = self.primaries_by_node().get(victim, [])
        apps_before = self.app_pods_by_node()
        apps_spread = all(set(experiments.DEPLOYMENTS) <= set(apps_before.get(worker, {}))
                          for worker in self.cluster.workers())

        def disruption(run_id: str, generators: list) -> dict[str, Any]:
            sampler = WritableSampler(self.k8s, avoid_node=victim)
            sampler.start()
            time.sleep(6)
            log(f"power cut on {victim} (primaries there: {primaries_on_victim})")
            self.cluster.interrupt(victim)
            cut = time.monotonic()
            cut_at = round(cut - generators[0].started_at, 1)
            failovers = {}
            try:
                for item in primaries_on_victim:
                    cluster, old = item.split(":")
                    seconds = self.wait_until(
                        lambda: self.k8s.leader(cluster)[0] != old
                        and self.k8s.psql(cluster, "SELECT NOT pg_is_in_recovery()") == [["t"]],
                        180, f"{cluster} failover", interval=1)
                    failovers[cluster] = {
                        "old_leader": old, "new_leader": self.k8s.leader(cluster)[0],
                        "seconds_after_power_cut": round(time.monotonic() - cut, 1),
                        "wait": seconds,
                    }
                    log(f"{cluster}: {failovers[cluster]}")
                # Shard apps are unready while their database cannot commit, so
                # this is measured, not required; the load shows actual service.
                try:
                    self.wait_until(
                        lambda: all(any(pod["spec"].get("nodeName") != victim
                                        for pod in self.k8s.ready_pods(name))
                                    for name in experiments.DEPLOYMENTS),
                        max(1.0, outage - (time.monotonic() - cut)),
                        "applications ready on the healthy worker", interval=2)
                    serving = round(time.monotonic() - cut, 1)
                except TimeoutError:
                    serving = None
                remaining = max(0.0, outage - (time.monotonic() - cut))
                time.sleep(remaining)
                apps_during = self.app_pods_by_node()
                during = self.layout_snapshot()
            except BaseException:
                self.cluster.resume(victim)
                raise
            log(f"power back on {victim}")
            self.cluster.resume(victim)
            resumed = time.monotonic()
            resumed_at = round(resumed - generators[0].started_at, 1)
            redundancy = self.wait_until(self.fully_redundant, 900,
                                         "full redundancy after the worker returned")
            time.sleep(10)
            sampler.stop()
            samples = sampler.summary()
            return {
                "passed": samples["never_two_writable"] and bool(failovers or not primaries_on_victim)
                and apps_spread,
                "apps_on_both_workers_before": apps_spread,
                "interrupted_worker": victim,
                "primaries_on_interrupted_worker": primaries_on_victim,
                "failovers": failovers,
                "seconds_until_every_app_tier_ready_on_healthy_worker": serving,
                "ready_app_pods_per_node_before": apps_before,
                "ready_app_pods_per_node_during_outage": apps_during,
                "databases_during_outage": during,
                "seconds_until_full_redundancy_after_power_on": redundancy,
                "seconds_powered_off": round(resumed - cut, 1),
                "load_clock": {"power_cut_at": cut_at, "power_on_at": resumed_at},
                "acknowledged_while_worker_down": acknowledged_between(generators[0], cut_at, resumed_at),
                "writable_primary_samples": samples,
                "databases_after": self.layout_snapshot(),
            }

        result = self.under_load("worker-interrupt", disruption)
        result["databases_before"] = before
        return result

    def scenario_worker_destroy(self, node: str | None, failpoint_seconds: float) -> dict[str, Any]:
        """Bonus II-A: a worker VM and its disks are destroyed mid-transaction."""
        self.wait_until(self.fully_redundant, 600, "a fully redundant starting layout")
        self.spread_apps()
        victim = self.pick_worker(node)
        before = self.layout_snapshot()
        primaries_on_victim = self.primaries_by_node().get(victim, [])
        # Every cross-shard transfer pauses after PREPARE and after the COMMIT
        # decision, so the destruction is certain to interrupt some of them.
        self.set_failpoints(failpoint_seconds)

        def disruption(run_id: str, generators: list) -> dict[str, Any]:
            sampler = WritableSampler(self.k8s, avoid_node=victim)
            sampler.start()
            time.sleep(10)
            in_flight = self.in_flight_transfers()
            log(f"destroying {victim} and its disks; {len(in_flight)} transfers in flight, "
                f"primaries there: {primaries_on_victim}")
            self.cluster.destroy(victim)
            cut = time.monotonic()
            cut_at = round(cut - generators[0].started_at, 1)
            failovers = {}
            for item in primaries_on_victim:
                cluster, old = item.split(":")
                self.wait_until(
                    lambda: self.k8s.leader(cluster)[0] != old
                    and self.k8s.psql(cluster, "SELECT NOT pg_is_in_recovery()") == [["t"]],
                    180, f"{cluster} failover", interval=1)
                failovers[cluster] = {"old_leader": old, "new_leader": self.k8s.leader(cluster)[0],
                                      "seconds_after_destruction": round(time.monotonic() - cut, 1)}
                log(f"{cluster}: {failovers[cluster]}")
            self.wait_until(lambda: self.node_ready(victim) is False, 120, f"{victim} NotReady", 2)
            log(f"declaring {victim} permanently lost (delete Node)")
            self.cluster.forget(victim)
            self.drop_claims_on(victim)
            local = self.wait_until(lambda: self.fully_redundant(require_remote=False), 900,
                                    "redundancy rebuilt on the surviving worker")
            surviving = self.layout_snapshot()
            log(f"redundancy rebuilt on the surviving worker after {local} s; adding a new worker VM")
            self.set_failpoints(0)
            added_at = time.monotonic()
            new_worker = self.cluster.add_worker()
            remote = self.wait_until(self.fully_redundant, 1200,
                                     f"a synchronous standby on the new worker {new_worker}")
            sampler.stop()
            outcomes = self.transfer_outcomes([item["transaction_id"] for item in in_flight])
            atomic = all(
                (o["coordinator"] == "COMMITTED" and set(o["participants"].values()) <= {"committed"})
                or (o["coordinator"] == "ABORTED" and "committed" not in o["participants"].values())
                for o in outcomes.values()
            )
            backups = self.run_backups()
            samples = sampler.summary()
            return {
                "passed": samples["never_two_writable"] and atomic and all(backups.values()),
                "destroyed_worker": victim,
                "destroyed_at_load_second": cut_at,
                "primaries_on_destroyed_worker": primaries_on_victim,
                "failovers": failovers,
                "transfers_in_flight_at_destruction": in_flight,
                "their_final_state": outcomes,
                "in_flight_transfers_resolved_atomically": atomic,
                "seconds_until_redundancy_on_surviving_worker": local,
                "databases_on_surviving_worker": surviving,
                "new_worker": new_worker,
                "seconds_until_cross_worker_redundancy_after_adding_it": remote,
                "seconds_from_add_to_redundant": round(time.monotonic() - added_at, 1),
                "backups_after_recovery": backups,
                "writable_primary_samples": samples,
                "databases_after": self.layout_snapshot(),
            }

        try:
            result = self.under_load("worker-destroy", disruption)
        finally:
            self.set_failpoints(0)
        result["databases_before"] = before
        return result

    def scenario_control_plane_loss(self, restore_on: str | None, outage: float) -> dict[str, Any]:
        """Bonus II-B: the control-plane VM is destroyed; management is rebuilt on a worker."""
        self.wait_until(self.fully_redundant, 600, "a fully redundant starting layout")
        self.spread_apps()
        before = self.layout_snapshot()
        control_plane = self.cluster.control_plane_name()
        target = restore_on or next(node for node in self.cluster.workers() if node != control_plane)
        log("taking a control-plane backup (etcd snapshot + PKI) to the host and every worker")
        backup = self.cluster.backup_control_plane()

        def disruption(run_id: str, generators: list) -> dict[str, Any]:
            time.sleep(20)       # state keeps changing after the snapshot
            log(f"destroying the control-plane VM {control_plane}")
            self.cluster.destroy(control_plane)
            cut = time.monotonic()
            generator = generators[0]
            successes_before = len(generator.successes)
            restore_started = None
            try:
                time.sleep(outage)
                served = len(generator.successes) - successes_before
                api_down = self.k8s.run("get", "nodes", check=False, timeout=20) == ""
                log(f"{served} requests answered while the control plane was gone; "
                    f"restoring on {target}")
            finally:
                restore_started = time.monotonic()
                self.cluster.restore_control_plane(target, backup, old=control_plane)
            api_seconds = round(time.monotonic() - restore_started, 1)
            scheduling = self.prove_scheduling()
            redundancy = self.wait_until(self.fully_redundant, 600, "databases after the restore")
            return {
                "passed": api_down and served > 0 and scheduling["passed"],
                "destroyed_control_plane": control_plane,
                "control_plane_backup": backup,
                "api_unavailable_after_destruction": api_down,
                "requests_answered_while_control_plane_was_gone": served,
                "seconds_without_control_plane": round(time.monotonic() - cut, 1),
                "restored_on": target,
                "seconds_to_restore_api": api_seconds,
                "scheduling_after_restore": scheduling,
                "seconds_until_databases_redundant_after_restore": redundancy,
                "nodes_after": self.k8s.run("get", "nodes", "-o", "wide", check=False),
                "databases_after": self.layout_snapshot(),
            }

        result = self.under_load("control-plane-loss", disruption)
        result["databases_before"] = before
        return result

    # ------------------------------------------------------------- support
    def node_ready(self, node: str) -> bool | None:
        nodes = self.k8s.json("get", "nodes")["items"]
        for item in nodes:
            if item["metadata"]["name"] == node:
                return any(c["type"] == "Ready" and c["status"] == "True"
                           for c in item["status"]["conditions"])
        return None

    def drop_claims_on(self, node: str) -> None:
        """Backup claims bound to the lost worker are recreated on a live one
        (the database controller handles the database claims itself)."""
        claims = self.k8s.json("get", "pvc")["items"]
        for claim in claims:
            name = claim["metadata"]["name"]
            if name.startswith("data-") or not claim["spec"].get("volumeName"):
                continue
            volume = json.loads(self.k8s.run("get", "pv", claim["spec"]["volumeName"], "-o", "json",
                                             check=False) or "{}")
            terms = volume.get("spec", {}).get("nodeAffinity", {}).get("required", {}) \
                .get("nodeSelectorTerms", [])
            nodes = {value for term in terms for expression in term.get("matchExpressions", [])
                     for value in expression.get("values", [])}
            if node in nodes:
                log(f"backup claim {name} lived on {node}; recreating it")
                # Retain: the provisioner's cleanup Pod could never run there.
                self.k8s.run("patch", "pv", claim["spec"]["volumeName"], "-p",
                             '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}')
                # Re-applying while the old claim still terminates would be a
                # no-op, and the claim would then be gone for good.
                self.k8s.run("delete", "pvc", name, "--timeout=300s", timeout=320)
                self.k8s.run("delete", "pv", claim["spec"]["volumeName"], "--wait=false",
                             check=False)
        self.k8s.run("apply", "-k", str(experiments.ROOT / "k8s" / "overlays" / "multi-vm"),
                     timeout=120)

    def run_backups(self) -> dict[str, bool]:
        results = {}
        for cronjob in ("db-backup", "db-backup-slot-2"):
            job = f"{cronjob}-check-{int(time.time())}"
            self.k8s.run("create", "job", job, f"--from=cronjob/{cronjob}")
            ok = self.k8s.run("wait", "--for=condition=complete", f"job/{job}", "--timeout=300s",
                              check=False, timeout=320) != ""
            node = self.k8s.run("get", "pods", "-l", f"job-name={job}", "-o",
                                "jsonpath={.items[0].spec.nodeName}", check=False)
            results[f"{cronjob}@{node}"] = ok
        return results

    def prove_scheduling(self) -> dict[str, Any]:
        """After the restore: replace a deleted Pod and run a brand-new Job."""
        victim = self.k8s.ready_pods("shard-a-app")[0]
        self.k8s.run("delete", "pod", victim, "--wait=false")
        replaced = self.wait_until(
            lambda: victim not in self.k8s.ready_pods("shard-a-app")
            and len(self.k8s.ready_pods("shard-a-app")) >= 2,
            300, "a replacement shard-a-app Pod", interval=2)
        job = f"scheduling-check-{int(time.time())}"
        self.k8s.run("create", "job", job, f"--image={experiments_image(self.k8s)}", "--",
                     "python", "-c", "print('scheduled after control-plane restore')")
        completed = self.k8s.run("wait", "--for=condition=complete", f"job/{job}",
                                 "--timeout=240s", check=False, timeout=260) != ""
        node = self.k8s.run("get", "pods", "-l", f"job-name={job}", "-o",
                            "jsonpath={.items[0].spec.nodeName}", check=False)
        return {
            "passed": completed,
            "deleted_pod": victim,
            "seconds_until_replacement_ready": replaced,
            "new_job": job,
            "job_completed_on": node,
        }


def acknowledged_between(generator: experiments.LoadGenerator, start: float, end: float) -> dict[str, Any]:
    inside = sorted(moment for moment in generator.successes if start <= moment <= end)
    return {
        "count": len(inside),
        "first_after_failure_at": round(inside[0], 1) if inside else None,
    }


def experiments_image(k8s: experiments.Kubernetes) -> str:
    deployment = k8s.json("get", "deployment", "gateway")
    return deployment["spec"]["template"]["spec"]["containers"][0]["image"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0],
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("scenario", choices=["worker-interrupt", "worker-destroy", "control-plane-loss"])
    parser.add_argument("--gateway", default="http://127.0.0.1:18080")
    parser.add_argument("--profile", default="banking-mv")
    parser.add_argument("--namespace", default="banking")
    parser.add_argument("--node", help="worker to break (default: the one with most primaries)")
    parser.add_argument("--outage", type=float, default=180, help="seconds the VM stays down")
    parser.add_argument("--failpoint-seconds", type=float, default=2)
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--settle-timeout", type=float, default=600)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    args.context = f"{args.profile}-edge"
    args.duration = 40
    args.image = None
    runner = VmExperiments(args)
    try:
        runner.fund_accounts()
        if args.scenario == "worker-interrupt":
            result = runner.scenario_worker_interrupt(args.node, args.outage)
        elif args.scenario == "worker-destroy":
            result = runner.scenario_worker_destroy(args.node, args.failpoint_seconds)
        else:
            result = runner.scenario_control_plane_loss(args.node, args.outage)
    finally:
        runner.close()
    report = {"scenario": args.scenario,
              "started_at": datetime.now(timezone.utc).isoformat(), **result}
    text = json.dumps(report, indent=2, default=str)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    print(text)
    log(f"{args.scenario}: {'PASSED' if result['passed'] else 'FAILED'}")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
