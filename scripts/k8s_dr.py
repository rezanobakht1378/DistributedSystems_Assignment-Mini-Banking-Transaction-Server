"""Disaster recovery for the Kubernetes deployment.

backup   run the 2PC-consistent backup job now and copy the newest backup set
         off the cluster (to a directory on this machine / offsite storage)
restore  load a backup set into a freshly deployed, empty stack
         (`kubectl apply -k k8s/base` first), then let the apps start on it

Restore order: the applications and gateways are scaled to zero so nothing
writes while the dumps load; each database is restored on its current primary
(the synchronous standby receives the same WAL); then the Deployments are
scaled back and the HorizontalPodAutoscalers take over again. Because the
three dumps form one 2PC-consistent cut, the gateways' recovery loop finishes
any transfer that was PREPARING/COMMIT/ABORT at backup time.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATABASES = {
    "coordinator-db": ("banking_coordinator", "coordinator.dump"),
    "shard-a-db": ("banking_shard_a", "shard-a.dump"),
    "shard-b-db": ("banking_shard_b", "shard-b.dump"),
}
WRITERS = ("gateway", "shard-a-app", "shard-b-app")


def kubectl(namespace: str, *arguments: str, timeout: float = 300) -> str:
    result = subprocess.run(
        ["kubectl", "-n", namespace, *arguments],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(
            f"kubectl {' '.join(arguments)} failed: {result.stderr.strip() or result.stdout.strip()}"
        )
    return result.stdout.strip()


def leader(namespace: str, cluster: str) -> str:
    lease = json.loads(kubectl(namespace, "get", "lease", f"{cluster}-leader", "-o", "json"))
    return lease["spec"]["holderIdentity"]


def wait_writable(namespace: str, cluster: str, timeout: float = 300) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            pod = leader(namespace, cluster)
            output = kubectl(
                namespace, "exec", pod, "-c", "postgres", "--",
                "psql", "-U", "banking", "-d", "postgres", "-Atc", "SELECT NOT pg_is_in_recovery()",
            )
            if output == "t":
                return pod
        except RuntimeError:
            pass
        time.sleep(3)
    raise TimeoutError(f"{cluster} has no writable primary")


def backup(namespace: str, output: Path) -> Path:
    job = f"backup-dr-{datetime.now():%Y%m%d%H%M%S}"
    kubectl(namespace, "create", "job", job, "--from=cronjob/db-backup")
    kubectl(namespace, "wait", "--for=condition=complete", f"job/{job}", "--timeout=300s", timeout=320)
    print(kubectl(namespace, "logs", f"job/{job}"))
    kubectl(namespace, "apply", "-f", str(ROOT / "k8s" / "dr" / "backup-reader.yaml"))
    try:
        kubectl(namespace, "wait", "--for=condition=Ready", "pod/backup-reader", "--timeout=120s", timeout=140)
        newest = kubectl(
            namespace, "exec", "backup-reader", "--",
            "sh", "-c", "ls -1d /backups/20*Z | sort | tail -n 1",
        )
        stamp = newest.rsplit("/", 1)[1]
        target = output / stamp
        target.mkdir(parents=True, exist_ok=True)
        for _, filename in DATABASES.values():
            kubectl(namespace, "cp", f"backup-reader:{newest}/{filename}", str(target / filename))
    finally:
        kubectl(namespace, "delete", "pod", "backup-reader", "--wait=false")
    print(f"backup set copied to {target}")
    return target


def restore(namespace: str, source: Path) -> None:
    missing = [name for _, name in DATABASES.values() if not (source / name).is_file()]
    if missing:
        raise FileNotFoundError(f"{source} is missing {missing}")
    for deployment in WRITERS:
        kubectl(namespace, "scale", f"deployment/{deployment}", "--replicas=0")
    deadline = time.monotonic() + 120
    while kubectl(namespace, "get", "pods", "-o", "name", "-l",
                  "app.kubernetes.io/name in (" + ",".join(WRITERS) + ")"):
        if time.monotonic() > deadline:
            raise TimeoutError("application Pods did not stop")
        time.sleep(2)

    for cluster, (database, filename) in DATABASES.items():
        pod = wait_writable(namespace, cluster)
        remote = f"/tmp/{filename}"
        kubectl(namespace, "cp", str(source / filename), f"{pod}:{remote}", "-c", "postgres")
        kubectl(
            namespace, "exec", pod, "-c", "postgres", "--",
            "pg_restore", "-U", "banking", "-d", database, "--role=bank_app",
            "--no-owner", "--clean", "--if-exists", "--single-transaction", "--exit-on-error", remote,
            timeout=600,
        )
        kubectl(namespace, "exec", pod, "-c", "postgres", "--", "rm", "-f", remote)
        print(f"restored {filename} into {cluster} ({pod})")

    for deployment in WRITERS:
        kubectl(namespace, "scale", f"deployment/{deployment}", "--replicas=2")
    for deployment in WRITERS:
        kubectl(namespace, "rollout", "status", f"deployment/{deployment}", "--timeout=300s", timeout=320)
    print("restore complete; applications are serving the restored data")


FINGERPRINT_SQL = {
    "coordinator-db": (
        "SELECT json_build_object('transfers', count(*), 'states', "
        "md5(coalesce(string_agg(transaction_id || ':' || state, ',' ORDER BY transaction_id), ''))) "
        "FROM bank_coordinated_transfers"
    ),
    "shard": (
        "SELECT json_build_object('accounts', count(*), 'total', sum(balance), "
        "'balances', md5(string_agg(wallet || ':' || balance, ',' ORDER BY wallet)), "
        "'ledger_rows', (SELECT count(*) FROM bank_transactions), "
        "'operations', (SELECT count(*) FROM bank_operations), "
        "'participant_rows', (SELECT count(*) FROM bank_prepared_transfers)) FROM accounts"
    ),
}


def fingerprint(namespace: str) -> dict[str, dict]:
    result = {}
    for cluster, (database, _) in DATABASES.items():
        sql = FINGERPRINT_SQL.get(cluster, FINGERPRINT_SQL["shard"])
        output = kubectl(
            namespace, "exec", wait_writable(namespace, cluster), "-c", "postgres", "--",
            "psql", "-U", "banking", "-d", database, "-Atc", sql,
        )
        result[cluster] = json.loads(output)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--namespace", default="banking")
    commands = parser.add_subparsers(dest="command", required=True)
    backup_parser = commands.add_parser("backup", help="Back up now and copy the set off the cluster.")
    backup_parser.add_argument("--output", type=Path, default=ROOT / "backups")
    restore_parser = commands.add_parser("restore", help="Restore a backup set into a fresh deployment.")
    restore_parser.add_argument("source", type=Path, help="Directory containing the three .dump files.")
    commands.add_parser("fingerprint", help="Print counts and hashes of the live data for comparison.")
    args = parser.parse_args(argv)
    if args.command == "backup":
        backup(args.namespace, args.output)
    elif args.command == "restore":
        restore(args.namespace, args.source)
    else:
        print(json.dumps(fingerprint(args.namespace), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
