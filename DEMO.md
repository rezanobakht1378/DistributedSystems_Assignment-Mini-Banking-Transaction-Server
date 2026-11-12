# Demo script (Assignment 3, max 20 minutes)

The sections follow the order required by the assignment. Times are a
budget, not a measurement. Every command runs from the repository root in
PowerShell. Lines starting with `#` are what to say or point at.

Two clusters are used:

| Part | Cluster | kubectl context | Gateway URL |
|---|---|---|---|
| 1–7 | Minikube profile `banking`, one node | `banking` | `minikube -p banking service gateway -n banking --url` |
| 8–9 | Minikube profile `banking-mv`, three VMs | `banking-mv-edge` | `http://127.0.0.1:18080` |

Before recording:

```powershell
minikube -p banking start
kubectl config use-context banking
$env:BANK_GATEWAY_URL = (minikube -p banking service gateway -n banking --url)   # keep the tunnel window open
docker build -t mini-banking-app:1.3.6 . ; minikube -p banking image load mini-banking-app:1.3.6   # rolling-update target
```

## 1. Deployed system (1.5 min)

```powershell
kubectl -n banking get deploy,statefulset,svc,ingress,hpa,pdb,cronjob
kubectl -n banking get pods -o wide
kubectl -n banking get pvc
kubectl -n banking get networkpolicy
curl.exe -s "$env:BANK_GATEWAY_URL/health"
```

- Deployments: `gateway`, `shard-a-app` and `shard-b-app` (all scaled by an
  HPA) and `db-controller`.
- StatefulSets: `shard-a-db`, `shard-b-db` and `coordinator-db`, each with
  one PVC per Pod.
- External entry point: NodePort 30080 and Ingress `banking.local`.

## 2. Normal operations with shard-aware routing (2 min)

```powershell
python client/client.py balance A0001                 # shard A
python client/client.py balance A1001                 # shard B
python client/client.py deposit A0001 10
python client/client.py withdraw A1001 3
python client/client.py transfer A0001 A0002 2        # same shard: one local transaction
python client/client.py transfer A0001 A1002 5        # cross shard: 2PC through the coordinator
curl.exe -s -i "$env:BANK_GATEWAY_URL/balance/inquiry?account=A1001" | Select-String "X-Gateway-Replica|X-Backend-Replica|balance"
kubectl -n banking logs -l app.kubernetes.io/name=gateway --tail=20 | Select-String "2pc|transfer"
```

- `X-Backend-Replica` shows which shard replica answered.
- The shard map is the `banking-shard-map` ConfigMap: A0001–A1000 belong to
  shard A and A1001–A2000 to shard B.
- The gateway log shows the transfer's 2PC states: PREPARING, COMMIT and
  COMMITTED.

## 3. Replication to the standby (1 min)

```powershell
kubectl -n banking exec deploy/db-controller -- python db_controller.py status
```

- Each cluster has a leader, its epoch and the synchronous standby recorded
  in the Lease.
- `pg_stat_replication` reports the standby as `streaming/sync` with lag 0.
- Deposit again, then show that the standby holds the same balance:

```powershell
python client/client.py deposit A0003 1
kubectl -n banking exec shard-a-db-1 -c postgres -- psql -U banking -d banking_shard_a -c "select wallet, balance, pg_is_in_recovery() as standby from accounts where wallet = 'A0003'"
```

## 4. Failures and recovery (4 min)

Every run prints the DB checks: ledger per request ID, money conservation,
2PC states, and an atomic outcome on both participants.

```powershell
python scripts/k8s_experiments.py --gateway $env:BANK_GATEWAY_URL --scenarios pod-failures --output k8s-results/demo-pods.json
python scripts/k8s_experiments.py --gateway $env:BANK_GATEWAY_URL --scenarios db-failover:shard-a-db --output k8s-results/demo-db.json
python scripts/k8s_experiments.py --gateway $env:BANK_GATEWAY_URL --scenarios coordinator-interruption --output k8s-results/demo-coord.json
python scripts/summarize_results.py k8s-results/demo-coord.json
```

- **Pod kill:** the healthy replicas keep serving, with 0 seconds that had
  errors and no success.
- **Primary DB failure:** the Lease moves to the standby and the epoch goes
  up by one. The apps go unready but are not restarted. The old primary
  rejoins as a standby.
- **Coordinator interruption:**
  - a gateway killed after PREPARE: another gateway aborts the transfer;
  - a gateway killed after the COMMIT decision: another gateway finishes the
    commit.
- Query the transfer by its ID:
  `curl.exe -s "$env:BANK_GATEWAY_URL/transactions/<id>"`.

## 5. Autoscaling under real load (2.5 min)

```powershell
kubectl -n banking get hpa -w        # second window
python scripts/k8s_experiments.py --gateway $env:BANK_GATEWAY_URL --scenarios autoscale --output k8s-results/demo-autoscale.json
```

- The replica timeline goes from 2 to 4–6 and back.
- Throughput and latency are reported before, during and after scaling.
- All eleven database checks pass.

## 6. Rolling update and rollback under load (2 min)

```powershell
python scripts/k8s_experiments.py --gateway $env:BANK_GATEWAY_URL --scenarios rolling-update,rollback --image mini-banking-app:1.3.6 --output k8s-results/demo-update.json
kubectl -n banking rollout history deploy/shard-a-app
```

- `maxUnavailable: 0`, readiness gates, a preStop hook and a drain keep
  in-flight requests safe.
- A broken release stalls; `rollout undo` restores the previous version.

## 7. Repeated requests and the security restriction (1.5 min)

```powershell
python scripts/k8s_experiments.py --gateway $env:BANK_GATEWAY_URL --scenarios repeated-requests,network-policy --output k8s-results/demo-idem.json
python scripts/summarize_results.py k8s-results/demo-idem.json
```

- Repeated requests: 22 submissions of the same ID, one ledger effect and
  identical answers. A conflicting payload gets 409.
- NetworkPolicy:
  - shard-a-app cannot reach shard-b-db;
  - the gateway cannot reach any shard database.

## 8. Bonus I: three VMs (2.5 min)

```powershell
minikube -p banking stop
kubectl config use-context banking-mv-edge
kubectl get nodes -o wide                           # VM1 control plane (tainted), VM2 and VM3 workers
python scripts/k8s_multivm.py layout                # Pods per node, replication per database
python scripts/k8s_vm_experiments.py worker-interrupt --output k8s-results/demo-vm-interrupt.json
```

- Each database has a primary and a local standby on one worker, and a
  synchronous standby on the other worker. Backups run on both workers
  (`db-backup`, `db-backup-slot-2`).
- During the run, point at:
  - the failovers to the other worker;
  - the standby rebuilt on the healthy worker, after which writes resume;
  - `never_two_writable: true`;
  - the eleven database checks.

## 9. Bonus II: permanent VM loss (3 min, prerecord and speed up)

```powershell
python scripts/k8s_vm_experiments.py worker-destroy --output k8s-results/demo-vm-destroy.json
python scripts/k8s_vm_experiments.py control-plane-loss --output k8s-results/demo-cp-loss.json
kubectl get nodes -o wide
```

**Worker destroyed.** The container and its volume are removed while 2PC
transfers are paused at their failpoints. The run then shows:
1. failover to the surviving copies;
2. the in-flight transfers resolved atomically;
3. redundancy rebuilt on the surviving worker;
4. a new worker added, which receives a synchronous standby again;
5. backups running on both slots.

**Control plane destroyed.** The run shows:
1. applications keep serving while the API server is gone;
2. etcd and the PKI are restored from the bundle kept on the worker;
3. kubeadm rebuilds the control plane on that worker;
4. Kubernetes manages Pods again: it replaces a deleted Pod and runs a new
   Job;
5. the database checks pass.
