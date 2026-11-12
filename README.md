# Distributed Systems: Assingment 1: Mini Banking Transaction Server
# Reza Nobakht
## student id: 404724008

## Design

The service uses JSON over HTTP. Clients send balance-changing operations to
`POST /transactions`, for example
`{"action":"withdraw","account":"A1001","amount":200}`. The server returns a
JSON success or a structured error. Balance inquiry uses
`GET /balance/inquiry?account=A1001`; deposits, withdrawals, and transfers use
`POST /transactions` with an `action` field. A transfer includes a
`destination`, for example
`{"action":"transfer","account":"A1001","destination":"A1002","amount":25}`.
`amount` is a generic unit, not a dollar amount. The server passes it as a
Python `Decimal` and stores values in PostgreSQL `NUMERIC(20, 2)` to avoid
floating-point rounding errors. All accounts and customers are fictional.

FastAPI handles concurrent requests, while `BANK_MAX_CLIENTS` limits the
number of requests executing in the application at once. SQLAlchemy ORM
sessions manage PostgreSQL connections and map accounts, transactions, and
idempotency records to model classes. A write runs in a database transaction
and locks affected account rows with `SELECT ... FOR UPDATE`; simultaneous
updates therefore serialize and each withdrawal checks the latest committed
balance.
`X-Request-ID` is the idempotency key: retries with the same ID and payload
return the saved result; reusing an ID for a different operation is rejected.

## ACID and errors

* **Atomicity:** account changes, transaction records, and the saved
  idempotency response are committed in one PostgreSQL transaction. A failed
  operation rolls back all of them, including both sides of a transfer.
* **Consistency:** balances use PostgreSQL numeric values with a database
  constraint preventing negative values. Amounts must be positive and have at
  most two decimal places. Deposits and withdrawals create or remove money;
  transfers move the same amount between accounts without changing the total.
* **Isolation:** PostgreSQL row locks serialize concurrent transactions that
  update the same account; readers see committed state. The
  concurrent-withdrawal test checks that successful operations cannot
  overspend the account.
* **Durability:** PostgreSQL commits the account state, transaction history,
  and idempotency record before success is returned. Configure PostgreSQL with
  its normal durable WAL settings and retain its data directory across server
  restarts.

Unknown accounts, insufficient funds, invalid or missing fields, unsupported
actions, malformed JSON, and reused request IDs are returned as structured
errors. Middleware logs each request with its ID, client IP, action, account,
result, and latency to `banking_server.log` and standard output.

## Primary/backup and split-brain bonus

In cluster mode, the primary sends each write to the backup and waits for the
backup's PostgreSQL commit before committing locally and replying success. Both
nodes use the same request ID, so a client retry after a lost response cannot
apply the operation twice. The backup promotes itself only after acquiring an
exclusive PostgreSQL advisory lock from a third, independent witness database.
Both banking servers must use the same `BANK_WITNESS_DATABASE_URL` and
`BANK_CLUSTER_ID`; each keeps its own PostgreSQL banking database. The witness
records a monotonically increasing leadership epoch. Every replicated write
carries that epoch, and servers reject requests from an old leader.

If the servers lose contact but can still reach the witness, the current
primary retains the lock and the backup stays read-only. If the primary crashes,
PostgreSQL releases its session lock and the backup can acquire it and promote.
If a former primary returns, it cannot take the lock from the new primary. It
first downloads the active node's full account, transaction, and idempotency
snapshot, then remains a standby. A primary that cannot confirm its witness or
replica fails closed. A restarted primary with an existing witness term waits
for the backup to confirm or recover the latest state before it can take
leadership, avoiding promotion of a stale local database. When the
inter-server connection returns, the active primary synchronizes the standby
before reopening writes. The client tries the configured server URLs and
reuses a request ID across retries.

The witness must run on an independent host, remain reachable from both server
VMs during the server-to-server partition, and use durable PostgreSQL storage.
If the witness is unavailable, neither node may take over without a confirmed
lock. Restrict the witness and internal replication/snapshot routes to the
trusted VM network with firewall rules; these internal routes do not use a
shared application secret.

## Run

### Docker Compose

Start the gateway, two application replicas per shard, and the two independent
PostgreSQL databases:

```text
docker compose up --build -d
```

The gateway is available on the host at `http://localhost:18080` (container
port `8000`). Set `BANK_HTTP_PORT` to change the host port if needed. Configure
the client with this single gateway URL; it never connects to shard app
replicas directly. The gateway handles account routing, health checks, and
replica failover.

The explicit [shard-map.json](./shard-map.json) assigns `A0001`–`A1000` to
Shard A and `A1001`–`A2000` to Shard B. The gateway reads this map and
round-robins requests among that shard's replicas, checking `/health` before
forwarding and skipping failed replicas. Both replicas for a shard use the
same PostgreSQL database. The databases use separate Compose volumes,
`shard-a-data` and `shard-b-data`; the coordinator's durable transaction log
has its own `coordinator-data` volume. Each shard database is initialized
only with sample accounts in its own range (`A0001`–`A0002` on A;
`A1001`–`A1003` on B). Same-shard transfers lock both account rows and update
balances, transaction history, and the idempotency result in one PostgreSQL
transaction. The CLI client is built from `client/` and can be run as a
one-off command:

```text
docker compose run --rm client balance A1001
docker compose run --rm client deposit A1001 25.50
docker compose run --rm client withdraw A1001 10
docker compose run --rm client transfer A1001 A1002 5
```

Use `docker compose run --rm client transfer A0001 A0002 5` to exercise a
Shard A transfer. Cross-shard transfers use the gateway as a 2PC coordinator:
it records participant votes and the final decision in the persistent
coordinator database before sending COMMIT or ABORT. Each shard persists
prepared reservations and participant results so messages can be retried
safely. After a gateway restart, unresolved PREPARING transactions are
durably aborted, while durable COMMIT decisions are retried until both
participants confirm. The gateway returns success only after both commit
acknowledgements. Query progress with `GET /transactions/{request_id}`.
The response includes a public `status`: `PENDING` while a transaction is
being resolved, `COMPLETED` after commit, `ABORTED` after abort, or `UNKNOWN`
when the ID has not been recorded. The internal 2PC `state`, votes, and
participant acknowledgements are included for diagnostics; unknown IDs return
HTTP 200 with `status: "UNKNOWN"` so clients can poll without treating an
unseen ID as a transport error.
App replicas can be individually stopped to verify that the gateway skips
failed replicas and uses a healthy one.

Stop the services with `docker compose down`. Both database volumes are
retained; use `docker compose down -v` only when intentionally deleting shard
data. To run the server outside Compose with sharding enabled, set
`BANK_SHARD_ID` (`shard-a` or `shard-b`) and set `DATABASE_URL` to that
shard's database. `BANK_SHARD_ID` makes an app reject accounts owned by the
other shard and seeds only sample accounts in its own range. `gateway.py`
uses `BANK_SHARD_A_REPLICAS` and `BANK_SHARD_B_REPLICAS` for its upstream
replica lists. For legacy single-database mode, omit `BANK_SHARD_ID`. This
Compose setup does not configure primary/backup replication or its separate
witness database. Keep the coordinator database available and persistent:
losing its durable decisions while participants are prepared can leave
transfers unresolved.

### Required Assignment 2 experiments

The experiment driver automates all seven required scenarios: shard placement,
replica load distribution, concurrent writes through replicas, replica
interruption while concurrent requests are in flight and failover to a healthy
replica, same- and cross-shard transfers, Coordinator crash
recovery after durable COMMIT, and concurrent overlapping cross-shard
transfers. It checks account placement in both PostgreSQL databases, returned
balances and transaction states, participant acknowledgements, and money
conservation. Gateway responses include `X-Backend-Replica`, and the gateway
logs the selected replica with each forwarded request so routing is visible.

Start the Compose deployment and run the experiments from the repository root:

```powershell
docker compose up --build -d
python scripts\assignment2_experiments.py --gateway http://localhost:18080 --requests 20 --output results\assignment2.json
```

By default, the interrupted-2PC experiment is skipped. To run it, enable the
demo-only pause after the Coordinator durably records COMMIT, recreate the
gateway, and run that scenario separately:

```powershell
$env:BANK_EXPERIMENT_PAUSE_AFTER_COMMIT_SECONDS = "30"
docker compose up -d --force-recreate gateway
python scripts\assignment2_experiments.py --gateway http://localhost:18080 --experiments interrupted-2pc
$env:BANK_EXPERIMENT_PAUSE_AFTER_COMMIT_SECONDS = "0"
docker compose up -d --force-recreate gateway
```

The script waits until the transaction status shows durable `COMMIT` with no
participant acknowledgements, stops and restarts the gateway, then verifies
that recovery completes both acknowledgements. Keep the pause enabled only for
this experiment. The replica-interruption scenario also stops and restarts
`shard-a-app-1` automatically. The experiments issue small deposits and
transfers against the sample accounts and retain all Compose volumes; use a
disposable assignment deployment, not production data. Individual scenarios
can be selected with `--experiments` and a comma-separated list of names:
`shard-distribution`, `replica-load`, `concurrent-replica`,
`replica-interruption`, `transfers`, `interrupted-2pc`, and `concurrent-2pc`.
The JSON report can be attached to the technical report as experiment results.

The custom `self-healing-controller` uses the Docker API to maintain the
configured minimum number of healthy application replicas. It persists managed
replica identities in the Coordinator database, uses a PostgreSQL advisory lock
to avoid duplicate controllers, and gives created application containers no
Docker restart policy. The controller is granted access to the host Docker
socket, so run this configuration only on a trusted development host. Run its
failure/recovery experiment separately:

```powershell
python scripts\assignment2_experiments.py --gateway http://localhost:18080 --experiments self-healing
```

That experiment stops an application replica, verifies that its replacement
serves traffic, crashes and restarts the controller while concurrent deposits
are submitted, and checks that the persisted replacement is recovered without
creating a second container with the same identity.

Install Python 3.10 or later and the pinned dependencies:

```text
python -m venv .venv
# Linux/macOS:
source .venv/bin/activate
# Windows PowerShell:
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

For the required two-VM deployment, bind the server to its network interface
(the default is `0.0.0.0`) and use the server VM's real IP addresses. On Linux,
configure the primary on Server A:

```text
export BANK_NODE_ID=server-a
export BANK_ROLE=primary
export BANK_PEER_URL=http://SERVER_B_IP:8000
export BANK_WITNESS_DATABASE_URL=postgresql://WITNESS_HOST:5432/banking_witness
export BANK_CLUSTER_ID=mini-banking-lab
export DATABASE_URL=postgresql://banking_user:change-me@127.0.0.1:5432/banking_db
export BANK_MAX_CLIENTS=50
python main.py
```

In PowerShell, the equivalent primary setup is:

```powershell
$env:BANK_NODE_ID = "server-a"
$env:BANK_ROLE = "primary"
$env:BANK_PEER_URL = "http://SERVER_B_IP:8000"
$env:BANK_WITNESS_DATABASE_URL = "postgresql://WITNESS_HOST:5432/banking_witness"
$env:BANK_CLUSTER_ID = "mini-banking-lab"
$env:DATABASE_URL = "postgresql://banking_user:change-me@POSTGRESQL_HOST:5432/banking_db"
$env:BANK_MAX_CLIENTS = "50"
python main.py
```

Configure the backup on Server B by using these Linux settings:

```text
export BANK_NODE_ID=server-b
export BANK_ROLE=backup
export BANK_PRIMARY_URL=http://SERVER_A_IP:8000
export BANK_WITNESS_DATABASE_URL=postgresql://WITNESS_HOST:5432/banking_witness
export BANK_CLUSTER_ID=mini-banking-lab
export DATABASE_URL=postgresql://banking_user:change-me@127.0.0.1:5432/banking_db
export BANK_MAX_CLIENTS=50
export BANK_HEARTBEAT_INTERVAL=1
export BANK_FAILURE_THRESHOLD=3
python main.py
```

Use equivalent `$env:BANK_...` assignments in PowerShell when Server B runs
Windows. Replication has no authentication, so restrict the endpoint to a
trusted network using the VM firewall.

Set `BANK_GATEWAY_URL` on the client machine to the gateway address, for
example `http://GATEWAY_IP:8000`. The client accepts one gateway URL; do not
configure shard replica or backend server URLs. Then run:

```text
python client/client.py balance A1001
python client/client.py deposit A1001 25.50
python client/client.py withdraw A1001 10
python client/client.py transfer A1001 A1002 5
```

Create a PostgreSQL database and user for each server, then set `DATABASE_URL`
to that server's connection string. Keep both data directories persistent.
Create the separate witness database on a third machine reachable by both
servers, and configure the same witness URL and cluster ID on both. For a
single server, omit the cluster variables. `BANK_MAX_CLIENTS` configures
concurrent request handling. Tests create isolated temporary schemas in the
dedicated PostgreSQL database named by `BANK_TEST_DATABASE_URL` and run with
`python -m unittest discover -s tests`.

## Kubernetes deployment (Assignment 3)

The same code runs on Kubernetes, with sharding, per-shard replicas, the
shard-aware gateway and the 2PC coordinator unchanged. Everything is declared
in [k8s/base](./k8s/base) and applied with kustomize.

| Workload | Kind | Replicas | Role |
|---|---|---|---|
| `gateway` | Deployment + HPA (2–4) | stateless | public entry point (NodePort 30080 / Ingress `banking.local`), shard routing, 2PC coordinator |
| `shard-a-app`, `shard-b-app` | Deployment + HPA (2–6) | stateless | shard application replicas |
| `shard-a-db`, `shard-b-db`, `coordinator-db` | StatefulSet, 2 Pods each | stateful | PostgreSQL primary + synchronous streaming standby, 1 Gi PVC per Pod |
| `db-controller` | Deployment (2, one acts at a time via the `db-controller` Lease) | stateless | leader election records, failover, fencing, `-rw` Service routing, standby placement and rebuilds |
| `prometheus` | Deployment (1) | - | metrics scraping and alert rules |
| `db-backup` | CronJob (every 15 min) | - | 2PC-consistent `pg_dump` of all three databases |

### Deploy on Minikube

```powershell
minikube start -p banking --driver=docker --cni=calico --cpus=4 --memory=6144
minikube -p banking addons enable metrics-server
docker build -t mini-banking-app:1.3.5 .
minikube -p banking image load mini-banking-app:1.3.5
kubectl apply -k k8s/base
kubectl -n banking get pods -w                     # wait until all are Running/Ready
minikube -p banking service gateway -n banking --url
```

The tag deployed is `newTag` in [k8s/base/kustomization.yaml](./k8s/base/kustomization.yaml).
To use Docker Hub instead of side-loading:

```powershell
docker login
python scripts/publish_images.py --user <dockerhub-user>   # builds, pushes <user>/mini-banking-app:<tag> and :latest
kubectl apply -k k8s/overlays/dockerhub                    # or k8s/overlays/multi-vm-dockerhub
```

`publish_images.py` writes those two overlays. Each only overrides the
image to `docker.io/<user>/mini-banking-app:<tag>`.

Calico is required because the default Minikube network does not enforce
NetworkPolicy. Set `BANK_GATEWAY_URL` to the printed URL and use the existing
client unchanged, for example `python client/client.py transfer A0001 A1002 5`.
[k8s/base/secret.dev.yaml](./k8s/base/secret.dev.yaml) holds development
passwords only. Replace it with real Secrets, for example from a secret
manager or `kubectl create secret`, before using the manifests anywhere else.

### Service discovery and routing

Both the gateway and the shard apps read the shard map from one ConfigMap, so
the two can never disagree about ownership. Each shard app has a *headless*
Service. The gateway resolves it through DNS, which returns only **Ready**
Pods, round-robins over them and re-resolves at most once per second. Pods
created by scaling or updates join the rotation automatically, and Pods that
are deleted or unready drop out of it. The gateway also checks a replica's
`/health` before forwarding a request to it.

Retries are safe because every request carries its `X-Request-ID`:
- If the connection to a replica fails, the gateway retries the request on
  another replica.
- If a replica answers `server_busy`, the gateway tries another replica.
- If a replica accepted the request but timed out, the gateway does **not**
  send it elsewhere. It returns `503 upstream_timeout`, and the client retries
  with the same ID. The retry returns the operation's single stored result
  and never applies it twice.

Several gateway replicas can coordinate transfers at once. Each transfer is
owned by exactly one gateway at a time through a PostgreSQL advisory lock
held on a dedicated session in the coordinator database:
- A second gateway that receives the same transfer answers
  `503 transaction_in_progress`.
- The recovery loop of every gateway takes over only *orphaned* transfers. If
  the owning Pod dies, its session and lock disappear, and another gateway
  finishes the transfer from the durable coordinator log.

### Databases, replication and recovery

Each database is a two-Pod StatefulSet with a PersistentVolumeClaim per Pod.
The standby is a real PostgreSQL physical replica: it is cloned with
`pg_basebackup` and kept current by WAL streaming. Replication is
**synchronous**: the controller names exactly one standby in
`synchronous_standby_names`, so a commit, and therefore every success the
bank reports, is acknowledged only after that standby has flushed its WAL.
The controller records that standby in the Lease only once PostgreSQL
reports it as `sync`, and clears the record before switching to another
standby. Promoting the recorded standby therefore never loses an
acknowledged operation. An isolated old primary cannot acknowledge any write
either, because it has no synchronous standby.

Two controller replicas run; only the holder of the `db-controller` Lease
acts. It renews every 2 s and stops acting 6 s after its last successful
renewal. The other replica takes over only after the Lease has not changed
for 10 s, so two controllers never act at once. The controller probes
database Pods by Pod IP, so failover does not depend on cluster DNS.

The **leader record** of each cluster is a Kubernetes Lease named
`<cluster>-leader`:
- `holderIdentity` is the primary Pod, and `leaseTransitions` is the epoch.
- The record is changed only by compare-and-swap, so only one failover can
  ever win.
- Each Pod's init container reads it to decide whether to start as primary or
  as standby.

`db-controller` labels the leader `db-role=primary`. The
`<cluster>-rw` Service selects that label, and applications connect to it
with `target_session_attrs=read-write`.

**Automatic failover.** The controller probes every database every 2 s.
If the primary has been unreachable for 15 s
(`BANK_DB_FAILOVER_AFTER_SECONDS`), and the standby was last observed as
*synchronous*, the controller:
1. moves the Lease to the standby and increments the epoch;
2. fences the old primary: relabels it `replica` so it leaves the `-rw`
   Service, then deletes its Pod;
3. promotes the standby with `pg_promote()` and labels it `primary`.

The old primary's Pod is recreated, finds that it is no longer the leader,
discards its data (it may contain unacknowledged WAL) and re-clones itself
from the new primary as a synchronous standby. A database Pod that runs as
primary without being the leader is fenced by deleting its Pod. During the
outage the shard's app replicas fail readiness (`/readyz` reports
`database: down`) and return `503 database_unavailable`, but they are **not**
restarted. Clients retry with the same request ID.

**Recovery runbook.**

* *Primary Pod or node failed.* No action is needed. Watch the failover
  with `kubectl -n banking logs deploy/db-controller` (the events are
  `failover_started`, `failover_completed` and `standby_synchronized`) and
  inspect the result with
  `kubectl -n banking exec deploy/db-controller -- python db_controller.py status`.
* *Planned switchover*, for example before node maintenance:
  `kubectl -n banking exec deploy/db-controller -- python db_controller.py failover shard-a-db`.
  It is refused unless the standby was observed in sync.
* *Standby lost for a long time.* With synchronous replication, writes on that
  shard block until a standby is back. To favour availability over zero data
  loss, run `ALTER SYSTEM SET synchronous_standby_names = ''; SELECT pg_reload_conf();`
  on the primary. The controller then disables automatic failover for that
  cluster (it logs `degraded`), because the standby may now be behind. Set
  the value back to `'*'` once the standby is streaming again.
* *Manual promotion of a standby that was never seen in sync* (`--force`) can
  lose acknowledged commits. Use it only when the primary's volume is gone and
  restoring a backup is worse.
* Never delete the `*-leader` Leases or the `data-*` PVCs of a running
  cluster. A recorded leader whose volume is empty refuses to start, rather
  than creating an empty database over committed state.

After a failover, unresolved transfers stay recoverable:
- Participants persist `prepared` reservations, and the coordinator persists
  its decision before sending COMMIT or ABORT.
- Gateways retry COMMIT and ABORT until both participants acknowledge.
- Clients that time out can query `GET /transactions/{id}`, or retry with the
  same ID.

### Self-healing and availability

* Deployments and StatefulSets recreate failed Pods. PodDisruptionBudgets
  keep at least one replica of each tier during voluntary disruptions.
* **Liveness** (`/livez`) is independent of the database, and probes bypass
  the request-slot limit. An overloaded replica or a database outage
  therefore makes a replica *unready* without getting it restarted.
* **Readiness** (`/readyz`) requires an initialized, writable database. An
  unready replica disappears from the headless Service, so the gateway stops
  routing to it.
* Database errors are classified. Connection failures mark the database down.
  Lock waits (`lock_timeout` is 3 s) and pool exhaustion return the retryable
  `503 database_busy` and do not change readiness.
* `idle_in_transaction_session_timeout` and `tcp_user_timeout` make PostgreSQL
  release the row locks of a Pod that was killed mid-transaction within
  seconds.

### Autoscaling

Every container has CPU and memory requests and limits. HorizontalPodAutoscalers
scale the shard apps (2–6 replicas) and the gateways (2–4 replicas) at 60 %
CPU, which requires the metrics-server addon. Scaling up is immediate;
scaling down waits for a 90 s stabilization window. Correctness does not
depend on the replica count for three reasons:
- the apps are stateless;
- per-request advisory locks and row locks live in PostgreSQL;
- 2PC ownership is cluster-wide.

Each shard replica runs as many worker threads as it has request slots
(`BANK_MAX_CLIENTS`) and rejects excess requests with `server_busy` instead
of queueing them. The connection budget stays below `max_connections`: at
most 6 replicas × (10 + 6 + 1) connections per shard database.

### Rolling updates and rollback

Deployments use `RollingUpdate` with `maxUnavailable: 0`, `maxSurge: 1` and
`minReadySeconds: 5`. A new Pod must be Ready before an old one is removed.
On termination, a Pod first fails readiness and sleeps 8 s in `preStop`, so
DNS and the gateways stop sending it traffic. uvicorn then drains in-flight
requests for up to 25 s (`BANK_GRACEFUL_SHUTDOWN_SECONDS`) before exiting.

```powershell
# release: build, load, then bump newTag in k8s/base/kustomization.yaml
docker build -t mini-banking-app:1.3.6 . ; minikube -p banking image load mini-banking-app:1.3.6
kubectl apply -k k8s/base
kubectl -n banking rollout status deploy/shard-a-app

# a broken release (readiness never succeeds) stalls with the old Pods serving
kubectl -n banking set env deploy/shard-a-app BANK_FAIL_READINESS=true
kubectl -n banking rollout undo deploy/shard-a-app
```

The database StatefulSets use `OnDelete`. To update PostgreSQL:
1. Delete the standby Pod and wait for it to resynchronize.
2. Run a planned switchover; this replaces the old primary.

### Configuration, security and observability

* **Config.** Settings live in the `banking-config` ConfigMap and the shard
  map in `banking-shard-map`. Passwords live in the `banking-db-credentials`
  Secret: superuser, replication, and an unprivileged application role
  `bank_app`.
* **Network restrictions** are enforced by Calico. A default-deny ingress
  policy applies to the namespace, and egress is restricted for the apps:
  - Only the gateway is reachable from outside.
  - Shard apps accept traffic only from the gateway (and Prometheus).
  - Each shard app can reach only its own database.
  - The gateway can reach the shard apps and the coordinator database, but no
    shard database.
  - Each database accepts only its own app, its peer, `db-controller` and the
    backup job.
* **Least privilege:**
  - The gateway, shard app and controller Pods run as non-root with a
    read-only root filesystem and all capabilities dropped. PostgreSQL runs
    as the image's `postgres` user.
  - Only `db-controller` and the database init containers have Kubernetes API
    access, limited by RBAC to Leases and Pod labels.
* **Logs.** Each request is logged with its request ID, node, action, account,
  result and latency, along with replica discovery changes, `2pc` state
  transitions, database state changes, and the controller's failover and
  fencing events: `kubectl -n banking logs -l app.kubernetes.io/name=gateway`.
* **Metrics** are exposed on `/metrics` and scraped by Prometheus
  (`kubectl -n banking port-forward svc/prometheus 9090`):
  - requests by result and latency;
  - discovered and ready replicas;
  - database reachability and writability, replication lag, synchronous
    standby connected, leader epoch, failovers and fencing;
  - 2PC decisions, outcomes and recovery actions, and transactions by state.

  Alert rules cover missing replicas, databases that are unavailable or not
  writable, a missing standby, failovers, unresolved 2PC transactions and
  gateway 5xx responses.

### Required failure experiments

All seven experiments are repeatable with one command. The only prerequisites
are a deployed stack, the gateway URL, and a second image tag loaded into
Minikube for the rolling update:

```powershell
minikube -p banking service gateway -n banking --url        # keep it open, prints the gateway URL
docker build -t mini-banking-app:1.3.6 . ; minikube -p banking image load mini-banking-app:1.3.6
python scripts/k8s_experiments.py --gateway <URL> --scenarios required --image mini-banking-app:1.3.6 --output k8s-results/required.json
```

A single experiment runs with, for example, `--scenarios coordinator-interruption`.
`--scenarios all` adds the extra scenarios listed further down. A run takes
about 25 minutes, most of it the autoscaler's scale-down.

The script ([scripts/k8s_experiments.py](./scripts/k8s_experiments.py))
consists of three parts.

**Load generator.** By default 8 closed-loop client threads
(`--concurrency`) send requests through the public gateway before, during
and after every disruption. Timed disruptions last 40 s (`--duration`).
- the mix is deposits, same-shard transfers and about 40 % cross-shard
  transfers over a fixed set of accounts on both shards;
- every operation has its own `X-Request-ID`;
- like a real client, a thread retries with the same ID after a network error
  or a 5xx until it gets a definitive answer: success, `insufficient_funds`
  or `transaction_aborted`.

The experiments also send bursts of requests released at the same instant
(a barrier and one thread each), and raw single-attempt requests where a
client retry must not interfere.

**Disruption.** Each experiment injects its disruption with `kubectl`, or
with `ctr` on the node, while the load runs.

**Verification against the databases.** After the load stops and every
operation has an answer, the script waits until the coordinator has no
unresolved transfer. Then it reads the *current* primary of every database
with `psql` inside the Pod; it locates the primary through the Lease, not
through the Service. Every experiment must pass all eleven checks:

| Check | What is compared |
|---|---|
| `all_operations_resolved` | every client operation got a definitive answer |
| `no_unexpected_errors` | no answer other than success, `insufficient_funds` or `transaction_aborted` |
| `ledger_matches_acknowledged_operations` | per request ID, `bank_transactions` holds exactly the rows its answer implies: deposit or withdrawal 1, same-shard transfer 2, cross-shard transfer 1 on each shard; rejected or aborted operations have 0 |
| `no_duplicate_effects` | no request ID has two ledger rows for the same account and entry |
| `money_conserved` | the sum of balances equals the sum before + acknowledged deposits − acknowledged withdrawals |
| `no_negative_balances` | no tracked account is below zero |
| `ledger_chain_intact` | each account's rows chain from `balance_before` to `balance_after` without gaps, ending at `accounts.balance` |
| `2pc_outcomes_match_client_answers` | `bank_coordinated_transfers.state` is COMMITTED exactly for the transfers the client was told succeeded, ABORTED for the rest |
| `2pc_all_terminal` | no PREPARING, COMMIT or ABORT row is left on the coordinator, and no `prepared` row on any participant |
| `2pc_atomic_on_participants` | for every transfer, `bank_prepared_transfers` is committed on both shards or on neither |
| `repeated_requests_got_identical_answers` | every submission of the same ID received the same response body |

Each experiment adds its own checks, described below.

**1. Application Pod failure** (`pod-failures`)
- *Disruption:* every 8 s for 40 s, force-kills a random ready Pod with
  `--grace-period=0 --force`, cycling through the gateway, shard-a-app and
  shard-b-app.
- *Expected:* the remaining healthy replicas keep serving.
- *Verified:* the client timeline (`seconds_with_errors_and_no_success` must
  be 0); every killed Pod is replaced, and none of the killed Pods is among
  the ready ones afterwards; all eleven database checks.

**2. Primary database failure** (`db-failover:shard-a-db`, `db-failover:coordinator-db`)
- *Disruption:* freezes the primary's postgres container with
  `ctr -n k8s.io task pause`. A frozen process keeps its TCP connections
  open but never answers, which is harder than a clean crash.
- *Expected:* the db-controller detects the failure and fails over to the
  synchronous standby automatically, and the new primary holds every
  committed transaction.
- *Verified:*
  - the Lease moves to the other Pod and its epoch increases by exactly one;
  - the new primary answers `pg_is_in_recovery() = false`;
  - the old primary rejoins as synchronous standby after it is thawed;
  - application containers are **not** restarted: the app goes unready but
    stays alive;
  - against the new primary, every operation acknowledged before, during or
    after the failover has its ledger rows, nothing is duplicated, and money
    is conserved. Synchronous replication is what makes this hold: a commit
    is acknowledged only after the standby has it.

**3. Concurrent transactions** (`concurrent-transactions`)
- *Overdraft race:* the reserved account A0005 holds a balance `B`. Twenty
  withdrawals of `B/10` with distinct IDs are released at the same instant.
  Exactly 10 must succeed and 10 must get `insufficient_funds`. The final
  balance must be `B − 10·(B/10)` both through the API and in
  `accounts.balance` on the shard-a primary.
- *Colliding transfers:* 40 transfers released at once, in opposite
  directions over the same account pairs: A0001↔A1001 and A0002↔A1002
  cross-shard, A0001↔A0002 and A1001↔A1002 same-shard. These create
  lock-order conflicts between the 2PC participants and local transfers.
- *Load:* the normal 8-client background load runs throughout.
- *Verified:* exact balances, and all eleven database checks over every
  operation; the race and colliding operations are included.

**4. Coordinator interruption** (`coordinator-interruption`)
- *Setup:* the gateway has failpoints that sleep 30 s right after the source
  participant voted YES (durable state PREPARING), and right after the
  durable COMMIT decision, before any participant is told. The script enables
  both with `kubectl set env` and disables them again afterwards. Background
  load is single-shard so that it does not stop at the failpoints.
- *Case "after prepare":*
  1. A single-attempt client request starts a transfer A0005→A1005.
  2. The script polls the coordinator database until the row is PREPARING
     with `source_vote = true`; shard A holds a `prepared` debit.
  3. It finds the gateway that owns the transfer from `pg_locks`: the
     session advisory lock `bank-2pc:<id>`, mapped through
     `pg_stat_activity.client_addr` to a Pod IP.
  4. It force-kills that Pod.
  - *Expected:* another gateway takes over the transfer once the dead
    session's lock is released, and aborts it (presumed abort).
- *Case "after commit":* the same steps, with the gateway killed while the
  coordinator state is COMMIT, both participants are `prepared`, no
  participant has been told yet, and there are 0 ledger rows.
  - *Expected:* another gateway finishes the commit on both shards.
- *Verified per case:*
  - the final coordinator state;
  - both participant rows aborted, or both committed;
  - ledger rows: 0, or exactly 1 per shard;
  - the money that moved between A0005 and A1005: 0.00 or 1.00;
  - a client retry with the original ID returns `transaction_aborted` or
    the committed result;
  - the time until another gateway resolved the transfer;
  - all eleven database checks.

**5. Autoscaling under load** (`autoscale`)
- *Setup:* waits until every HPA is back at its minimum, then runs 2
  background clients plus a 48-client burst.
- *Expected:* the HPAs add replicas while the burst lasts, then remove them
  again while traffic continues, without breaking banking correctness.
- *Verified:* the replica timeline of all three HPAs during scale-up and
  scale-down. Pods are added and removed while requests are in flight, and
  removed Pods drain through the `preStop` hook and graceful shutdown. All
  eleven database checks run over the roughly 6000 operations.

**6. Rolling update under load** (`rolling-update`)
- *Disruption:* `kubectl set image` to the new tag on the gateway,
  shard-a-app and shard-b-app, one after the other, with `rollout status`.
- *Expected:* deployment changes preserve transaction integrity.
- *Verified:* every old Pod was replaced, and all running Pods run the new
  image. All eleven database checks pass; this includes the 2PC transfers
  whose gateway was replaced mid-flight, which the recovery loop finishes.

**7. Repeated client requests** (`repeated-requests`)
- *Cases:* a deposit, a same-shard transfer and a cross-shard transfer.
- *Steps for each case:*
  1. Sends the request with a 20 ms client timeout, so the response is lost
     but the request may still be applied.
  2. Sends 16 copies with the same ID simultaneously; the copies spread over
     both gateways.
  3. Sends 5 sequential replays.
  4. Sends the same ID with a different amount, which must get 409
     `request_id_conflict` and have no effect.
- *Expected:* no duplicate transaction effects.
- *Verified in the databases:*
  - the ledger rows for the ID: 1, 2, or 1+1;
  - the idempotency records: exactly one `bank_operations` row per shard
    involved, plus exactly one coordinator row for the cross-shard
    transfer;
  - all 21 answers are byte-identical;
  - all eleven database checks.

#### Results

**Environment.** The command above, run on images 1.2.0 → 1.2.1 on
Minikube with one node, 4 CPUs and Calico, 8 background clients and about
40 % cross-shard transfers.

**Reports.** The full report is
[k8s-results/required.json](./k8s-results/required.json), and
`python scripts/summarize_results.py k8s-results/required.json` condenses
it. All eight runs passed all eleven database checks. "Money" below is the
sum of all tracked balances read from the primaries, compared with
`before + acknowledged deposits − acknowledged withdrawals`.

**1. Application Pod failure: passed**
- *Injected:* 5 Pods force-killed, 8 s apart: 2 shard-a-app, 2 shard-b-app
  and 1 gateway.
- *Observed:*
  - There was never a second without a successful request.
  - Only 2 client retries were needed. The longest wait, 25.4 s, was one
    client whose connection was on the killed gateway; its retry with the
    same ID got the single result.
  - Full strength returned 9.1 s after the last kill, and none of the killed
    Pods was among the ready ones.
- *Database proof:*
  - 666 request IDs and 1186 ledger rows checked one by one.
  - Coordinator: 239 COMMITTED and 1 ABORTED, matching the 665 successes and
    1 `transaction_aborted` the clients received.
  - 480 participant rows, all committed or aborted in pairs.
  - Money 5642.78 + 8.71 = 5651.49, and the primaries hold exactly 5651.49.

**2. Primary database failure: passed**
- *Shard A, injected:* the primary shard-a-db-0 frozen with
  `ctr task pause`.
- *Shard A, observed:*
  - The controller failed over automatically after 21.1 s; shard-a-db-1
    became writable at 22.2 s, and the Lease epoch went 0 → 1.
  - The old primary rejoined as synchronous standby 14.7 s after it was
    thawed.
  - shard-a-app went unready (1 of 2 ready at the lowest), and no
    container restarted.
  - 45 client retries, 12 s with no success on any request; shard B kept
    serving.
- *Shard A, database proof on the new primary:*
  - all 255 request IDs have exactly the ledger rows their answers imply,
    450 rows in total;
  - 81 COMMITTED and 3 ABORTED transfers, matching the client answers, with
    participants committed or aborted in pairs;
  - money 5651.49 + 2.83 = 5654.32, exactly as stored. Every commit
    acknowledged before the freeze is present on the promoted standby.
- *Coordinator, injected:* the primary coordinator-db-0 frozen.
- *Coordinator, observed:*
  - failover after 22.4 s, writable at 23.3 s, epoch 0 → 1;
  - no gateway restarted, and all 4 stayed ready, because gateway readiness
    does not depend on the coordinator database;
  - cross-shard transfers blocked and retried (23 retries).
- *Coordinator, database proof:* all 87 transfers COMMITTED on the new
  coordinator primary and on both participants; money 5654.32 + 3.32 =
  5657.64, exact.

**3. Concurrent transactions: passed**
- *Injected, overdraft race:* A0005 held 109.00. 20 withdrawals of 10.90
  were released at the same instant.
- *Observed:* exactly 10 granted and 10 `insufficient_funds`. The balance is
  0.00 through the API *and* in `accounts.balance` on the shard-a primary.
- *Injected, colliding transfers:* 40 transfers in opposite directions over
  the same account pairs, cross-shard and same-shard, released together.
- *Observed:* all 40 succeeded, with no deadlock answer and no lost update.
- *Database proof:*
  - 292 request IDs and 510 ledger rows;
  - every account's ledger chains without gaps to its stored balance;
  - 124 transfers COMMITTED on all three databases;
  - money 5657.64 + 2.28 − 109.00 = 5550.92, exact;
  - no negative balance.

**4. Coordinator interruption: passed**
- *After prepare, injected:* the gateway owning transfer
  `…-after-prepare` was force-killed. At that moment the coordinator state
  was PREPARING with `source_vote = true`, shard A held a `prepared` debit,
  and there were 0 ledger rows.
- *After prepare, observed:* 17.1 s later another gateway had taken over
  the ownership lock and **aborted** the transfer.
- *After prepare, database proof:*
  - coordinator ABORTED, both acknowledgements true;
  - participants `debit = aborted`, `credit = aborted`;
  - 0 ledger rows on both shards, and A0005/A1005 unchanged (0.00 moved);
  - a client retry with the same ID returned `transaction_aborted`.
- *After commit, injected:* the owner of `…-after-commit` was killed. At
  that moment the coordinator state was COMMIT, both participants were
  `prepared`, and there were 0 ledger rows.
- *After commit, observed:* another gateway **finished the commit** after
  14.7 s.
- *After commit, database proof:*
  - coordinator COMMITTED, both acknowledgements true;
  - both participants `committed`, exactly 1 ledger row on each shard;
  - exactly 1.00 moved from A0005 to A1005;
  - a client retry with the same ID returned the stored success answer.
- *Background load:* 6716 single-shard operations, all verified.

**5. Autoscaling under load: passed**
- *Injected:* a 48-client burst on top of 2 background clients, starting
  with all three HPAs at their minimum of 2.
- *Observed:*
  - At 63 s the gateway went 2 → 4 and shard-a-app 2 → 3; at 111 s
    shard-a-app reached 5 and shard-b-app 4, and then shard-b-app 6.
  - After the burst, the shard apps scaled back 6 → 3 within 410 s while
    2 clients kept running. They returned to 2 afterwards; the gateway
    returned from 4 to 2.
  - No client retry was needed.
- *Database proof:*
  - 5013 request IDs, 8982 ledger rows;
  - 1493 transfers COMMITTED on coordinator and participants;
  - money 5637.78 + 55.56 = 5693.34, exact.

**6. Rolling update under load: passed**
- *Injected:* `kubectl set image` to `mini-banking-app:1.2.1` on
  shard-a-app (46.6 s), shard-b-app (40.1 s) and the gateway (54.8 s).
- *Observed:*
  - every old Pod was replaced, and all running Pods run 1.2.1;
  - no client retry, a longest wait of 2.8 s, and no second without
    success.
- *Database proof:*
  - 2101 request IDs, 3775 ledger rows;
  - all 817 cross-shard transfers COMMITTED on all three databases,
    including those whose gateway was replaced mid-flight;
  - money 5693.34 + 23.64 = 5716.98, exact.

**7. Repeated client requests: passed**
- *Injected:* for each of three request IDs, the original request with its
  response lost by a client timeout, 16 simultaneous copies spread over 4
  gateways, and 5 replays; 22 submissions per ID.
- *Observed:*
  - all 21 answers per ID are byte-identical;
  - reusing the ID with a different amount returned 409
    `request_id_conflict`.
- *Database proof, per request ID:*

| Request | Ledger rows (A / B) | `bank_operations` (A / B) | Coordinator rows |
|---|---|---|---|
| deposit | 1 / 0 | 1 / 0 | 0 |
| same-shard transfer | 2 / 0 | 1 / 0 | 0 |
| cross-shard transfer | 1 / 1 | 1 / 1 | 1 (COMMITTED) |

  Money 5716.98 + 5.20 = 5722.18, exact: each deposit counted once,
  although it was submitted 22 times.

### Additional scenarios

`--scenarios all` adds these scenarios to the required ones. The same
eleven database checks run after each of them:
- `network-policy`: a connectivity matrix from inside the Pods.
- `baseline`: load only, no disruption.
- `db-pod-delete:<db>`: deletes both Pods of a database cluster at once.
- `db-switchover:<db>`: a planned failover.
- `rollback`: deploys a broken release, then runs `rollout undo`.
- `backup`: runs the backup job under load.

The table below is from an earlier full run on images 1.1.x
(`k8s-results/full-run.json` and the per-scenario reports). It used
Minikube with one node, 4 CPUs and Calico, 8 concurrent clients and about
40 % cross-shard transfers. All invariants held in every scenario.

| Scenario | Operations (cross-shard) | Client retries | Longest wait for an answer | Observed |
|---|---|---|---|---|
| baseline | 604 (238) | 0 | 2.5 s | all committed |
| pod-failures | 423 (169) | 4 | 25.6 s ¹ | 5 Pods force-killed; full strength again 9 s after the last kill; no second without successful requests |
| db-pod-delete: shard-b-db | 86 (32) | 43 | 30.8 s | both DB Pods deleted; back with the same leader and data after 17 s, synchronous standby after 3 s more; 2 transfers aborted cleanly while shard B was down |
| db-switchover: coordinator-db | 276 (99) | 8 | 16.7 s | coordinator-db-1 promoted (epoch 0 → 1) in 10.5 s; old primary rejoined as synchronous standby 20 s later |
| db-failover: shard-a-db | 158 (65) | 55 | 43.7 s | frozen primary detected and failed over automatically in 23 s, writable after 24.5 s; shard-a-app Pods went unready but were **not** restarted; old primary rejoined in sync 10.5 s later; 4 transfers aborted cleanly |
| rollback | 461 (182) | 0 | 3.6 s | broken release never became Ready, all 4 old Pods kept serving; `rollout undo` took 1.9 s |
| rolling-update | 2655 (1101) | 1 | 5.2 s | all three Deployments replaced Pod by Pod under load |
| autoscale | 6445 (1916) | 0 | 19.6 s | 48 extra clients on an idle cluster: the HPAs scaled 2/2/2 → 4 gateways and 4+4 shard Pods at 100 s, then 6+6 shard Pods at 127 s; after the burst, shards scaled 6 → 4 while 2 clients kept running |
| backup | 242 (91) | 0 | 4.1 s | 2PC-consistent backup set written under load |

¹ A client connection to a gateway Pod killed with `--grace-period=0`
hangs until the client's 25 s timeout. The retry with the same ID then
returns the single result.

During a database outage the closed-loop clients eventually all wait on the
unavailable shard, so the "longest wait" column measures that shard's
recovery. The other shard keeps serving.

### Bonus: disaster recovery

The `db-backup` CronJob takes logical backups of all three databases every 15
minutes and keeps 24 sets. The dumps form one **2PC-consistent cut**:
- While a gateway owns a coordinated transfer, it holds a shared backup
  barrier, an advisory lock in the coordinator database.
- The backup job holds that barrier *exclusively* while it dumps the
  coordinator and both shards, so no 2PC state changes in any database during
  the dumps.
- Single-shard operations keep running. Each `pg_dump` is a transactionally
  consistent snapshot of its own database.

[scripts/k8s_dr.py](./scripts/k8s_dr.py) copies backups off the cluster and
restores them:

```powershell
python scripts/k8s_dr.py backup --output backups          # run a backup now and copy it off the cluster
# total loss, e.g. the namespace and every volume are gone:
kubectl apply -k k8s/base                                 # fresh, empty stack
python scripts/k8s_dr.py restore backups/<timestamp>
```

`restore` proceeds in four steps:
1. Scales the gateways and shard apps to zero.
2. Restores each dump on the current primary as `bank_app` in a single
   transaction; the synchronous standby replicates it.
3. Scales the apps back up, after which the HPAs take over again.
4. Leaves any transfer that was in flight at backup time to the gateways'
   recovery loop, which aborts `PREPARING` transfers and finishes `COMMIT`
   decisions.

The recovery point is the last backup, 15 minutes at most. Copy the sets to
storage outside the cluster, because the `db-backups` PVC is lost together
with the namespace.

`python scripts/k8s_dr.py fingerprint` prints row counts and md5 hashes of
every account balance, the ledger, the idempotency records and all 2PC state.
The drill on Minikube went as follows:
1. Took a backup under load and copied it to `backups/20261112T114604Z`.
   The fingerprint was saved as `k8s-results/dr/before.json`.
2. Ran `kubectl delete namespace banking`, which also deleted every
   PersistentVolume.
3. Ran `kubectl apply -k k8s/base`, which brought up a fresh stack holding
   only the seed data (`fresh.json`: 0 transfers, 0 ledger rows).
4. Ran `restore`.

The fingerprint after the restore (`after.json`) is **byte-identical** to
the one taken before the loss:

| Database | Restored state |
|---|---|
| coordinator-db | 6825 transfers, same state hash |
| shard-a-db | total 1665.02, 17321 ledger rows, 13017 idempotency records, 6825 participant rows, same balance hash |
| shard-b-db | total 3721.17, 16890 ledger rows, 12809 idempotency records, 6825 participant rows, same balance hash |

After the restore:
- all three standbys were streaming in sync with lag 0;
- the gateway's `/status` reported 6816 COMMITTED and 9 ABORTED transfers,
  with none unresolved.

Logs are in `k8s-results/dr/`.

### Bonus I: multi-VM cluster

The three VMs are Minikube nodes on the docker driver, in the profile
`banking-mv`. Each node has its own kubelet, containerd, IP address and disk
(a Docker volume), so killing a node container is a power cut, and removing
it together with its volume destroys the machine and its disks.

| VM | Node | Role |
|---|---|---|
| VM1 | `banking-mv` (192.168.67.2) | control plane, tainted `NoSchedule`; runs no application Pod |
| VM2 | `banking-mv-m02` | worker, backup slot 1 |
| VM3 | `banking-mv-m03` | worker, backup slot 2 |

A worker that replaces a destroyed one gets the next name (`-m04`, `-m05`,
...) and the free backup slot, which is why the results below name other
workers.

An edge proxy container outside the VMs ([scripts/edge_proxy.py](./scripts/edge_proxy.py))
plays the external load balancer:
- `http://127.0.0.1:18080` goes to the gateway NodePort on every worker;
- `https://127.0.0.1:18443` goes to whichever node runs an API server. The
  kubectl context `banking-mv-edge` uses it, so it keeps working after the
  control plane moves.

```powershell
docker build -t mini-banking-app:1.3.5 .
python scripts/k8s_multivm.py create          # 3 nodes, Calico, taint, edge proxy, images, add-on hardening
python scripts/k8s_multivm.py deploy          # kubectl apply -k k8s/overlays/multi-vm
python scripts/k8s_multivm.py layout          # Pods per VM, database roles, replication state
$env:BANK_GATEWAY_URL = "http://127.0.0.1:18080"
python client/client.py transfer A0001 A1002 5
```

[k8s/overlays/multi-vm](./k8s/overlays/multi-vm) changes the base as follows:
- **Three members per database**, each on its own `local-path` volume, that
  is, on the disk of the worker it runs on. With `BANK_DB_PLACEMENT=spread`
  the controller keeps a standby **on the primary's worker** and one **on
  the other worker**, and makes the remote one the synchronous standby. A
  topology spread (maxSkew 1) keeps the members on both workers.
- **Independent backups per worker.** The CronJobs `db-backup` and
  `db-backup-slot-2` write to volumes on the workers labelled
  `banking.local/backup-slot=1` and `=2`.
- **Application replicas on both workers.** Gateways and shard apps use a
  topology spread per revision (`matchLabelKeys: pod-template-hash`), and
  the two `db-controller` replicas use required anti-affinity. Stateless Pods
  leave an unreachable node after 20 s instead of 300 s.
- **Add-on hardening** (`k8s_multivm.py harden-addons`): CoreDNS,
  metrics-server and the volume provisioner stay off the control plane and
  leave a lost node after 10 s.

**Never two writable databases.** A member becomes primary only through a
compare-and-swap on its Lease, and only if it was the recorded synchronous
standby. An old primary that comes back sees that it is no longer the Lease
holder and is fenced: relabelled out of the `-rw` Service, deleted, and
re-cloned as a standby. While it is cut off it cannot acknowledge anything,
because its synchronous standby is on the other worker. The experiments
check this independently: a sampler asks every member every 2 s whether it
is writable.

**Writes while one worker is down.** After a failover the new primary's
only standby is on the dead worker, so commits block. Once that has lasted
20 s (`BANK_DB_REBUILD_UNREACHABLE_AFTER_SECONDS`) and the standby's node is
NotReady, the controller re-clones that standby, one at a time, on the
healthy worker. Its old copy holds nothing the new primary lacks. When the
worker returns, the placement logic moves one standby back to it.

#### Worker interrupted under load

```powershell
python scripts/k8s_vm_experiments.py worker-interrupt --output k8s-results/vm-worker-interrupt.json
```

The script picks the worker with the most primaries, cuts its power (`docker
kill`) under the closed-loop client load, keeps it off for 180 s and then
powers it back on. It reports the failovers, the writable samples, the
operations acknowledged during the outage, and the eleven database checks
used by the required experiments.

Result on image 1.3.5 ([k8s-results/vm-worker-interrupt.json](./k8s-results/vm-worker-interrupt.json)):

| | |
|---|---|
| Before the cut | worker `m04` held all three primaries and their local standbys; the synchronous standbys were on `m05`; 1 gateway, 1 shard-a and 1 shard-b replica on each worker |
| Power cut | 191 s, under 6 closed-loop clients |
| Failover | shard-a, shard-b and coordinator to their synchronous standbys on `m05` after 37.3, 41.7 and 44.6 s |
| Writes during the outage | the controller re-cloned one lost standby per database on `m05`; writes resumed 104 s after the cut, and 635 operations were acknowledged while `m04` was still off |
| After power-on | the old primaries came back as standbys of the new timeline; full cross-worker redundancy 113 s after power-on |
| Writable members | at most 1 per database in every 2 s sample (`never_two_writable: true`) |
| Database checks | all 11 passed: 1353 request IDs (1352 succeeded, 1 cross-shard transfer aborted); 2429 ledger rows; 523 COMMITTED and 1 ABORTED transfers with matching participant rows; money 5445.87 + 15.75 deposited = 5461.62 exactly |

The longest gap between two acknowledged operations was 104.9 s, and the
client retried 59 times with the same request IDs. Those retries hit
`503 shard_unavailable`, `database_unavailable` and `transaction_in_progress`,
and timeouts. No duplicate effect and no lost acknowledged operation
resulted. Writes block for that long because, right after the failover,
each new primary's only standby is on the dead worker, and the bank refuses
to commit with a single copy.

### Bonus II: disaster recovery across VMs

#### Scenario A: a worker and its disks are destroyed

```powershell
python scripts/k8s_vm_experiments.py worker-destroy --output k8s-results/vm-worker-destroy.json
```

1. Every cross-shard transfer is slowed down after PREPARE and after the
   COMMIT decision (a failpoint), so the destruction is certain to hit
   transfers in flight.
2. The worker holding the primaries is removed together with its Docker
   volume: `docker rm -f -v` and `docker volume rm`.
3. The controller fails over to the synchronous standbys on the surviving
   worker. Once the Node is deleted (`k8s_multivm.py forget`, declaring the
   machine gone), the controller drops the volumes of the lost members and
   re-clones them on the survivor. It deletes only the PersistentVolume
   objects: the provisioner's cleanup would need a Pod on the dead node.
4. A new worker VM joins (`k8s_multivm.py add-worker`, which falls back to
   its own `kubeadm join` if Minikube's step does not run). It gets the free
   backup slot, and the placement logic moves one standby per database onto
   it, which becomes the synchronous standby again.
5. Both backup CronJobs run once and must succeed.

The 2PC recovery loop in the gateways resolves the interrupted transfers:
PREPARING ones abort, and COMMIT decisions are finished on both shards. No
balance or decision is edited by hand.

Result on image 1.3.5 ([k8s-results/vm-worker-destroy.json](./k8s-results/vm-worker-destroy.json)):

| | |
|---|---|
| Destroyed | worker `m04` with its disk, 22 s into the load; it held all three primaries and their local standbys, plus the volume of backup slot 1 |
| In flight | 6 cross-shard transfers: 1 PREPARING and 5 with a durable COMMIT decision; 2 of them were owned by the gateway Pod on `m04` |
| Their outcome | the PREPARING one ABORTED on both shards; the 5 COMMIT decisions COMMITTED on both shards; all atomic |
| Failover | to the synchronous standbys on `m06` after 33.6, 36.5 and 39.3 s |
| Redundancy on the survivor | the Node was deleted 51 s after the destruction; the controller dropped the six lost members at once and re-cloned them on `m06`. Each database had a synchronous standby again 226 s later |
| New worker | `m07` joined and got backup slot 1; one standby per database moved onto it and became the synchronous standby 385 s after the join |
| Backups | `db-backup` on `m07` and `db-backup-slot-2` on `m06` both succeeded |
| Writable members | at most 1 per database in every 2 s sample |
| Database checks | all 11 passed: 1671 request IDs (1667 succeeded, 4 transfers aborted); 2994 ledger rows; 660 COMMITTED and 4 ABORTED transfers with matching participant rows; money 5505.69 + 19.44 = 5525.13 exactly |

Writes were blocked for 289 s after the destruction. For most of that time,
re-cloning waited for a volume: the local-path provisioner is a single
replica, it had been running on `m04`, and it had to be rescheduled on
`m06` first. In an earlier run, where the provisioner was on the surviving
worker, redundancy came back 108 s after the Node was deleted. Clients
retried 220 times with the same IDs and got `503 transaction_in_progress`
or `transaction_commit_pending` until the recovery loop had finished each
transfer. Each retry then received the single stored outcome.

#### Scenario B: the control-plane VM is destroyed

```powershell
python scripts/k8s_multivm.py backup-control-plane         # also done by the experiment
python scripts/k8s_vm_experiments.py control-plane-loss --output k8s-results/vm-control-plane-loss.json
```

`backup-control-plane` saves the following:
- an etcd snapshot (`etcdctl snapshot save`);
- the cluster PKI (CA, service-account keys, front-proxy and etcd CAs);
- the kubeadm configuration;
- the control-plane images.

It keeps copies on the host (`backups/control-plane/<timestamp>`,
gitignored because it holds the CA keys) and in `/var/lib/banking-dr` on
every worker, so a worker can rebuild management from its own disk.

`restore-control-plane --on <worker>` rebuilds management on a surviving
worker:
1. Restores the snapshot as a single-member etcd named after that worker
   (`etcdutl snapshot restore`).
2. Reuses the CA and keys, so every kubelet certificate and service-account
   token stays valid, and issues new API-server and etcd certificates for
   the worker's address.
3. Writes the static Pods with `kubeadm init phase control-plane all` and
   `etcd local`.
4. Points `control-plane.minikube.internal` at the new address on every
   node and restarts the kubelets.
5. Deletes the dead control-plane Node.

The worker keeps running workloads and is labelled
`node-role.kubernetes.io/control-plane-restored`.

Requests keep being served while there is no control plane: Pods, Services
and iptables rules stay in place. The two `db-controller` replicas stop
acting once they can no longer renew their Lease, so no failover can happen
during that time. The snapshot can be older than the database state. If a
failover happened after it was taken, the Lease names an old primary, and
the controller corrects the record from the members' timelines: the member
on the newer timeline wins (§ "Databases, replication and recovery").

Result ([k8s-results/vm-control-plane-loss.json](./k8s-results/vm-control-plane-loss.json),
on a freshly built three-VM cluster):

| | |
|---|---|
| Backup | taken under load 34 s before the destruction; copies on the host and on both workers |
| Destroyed | VM1 `banking-mv`, with its disk |
| Without management | 206 s until the restore started; `kubectl get nodes` failed |
| Served meanwhile | 2160 requests acknowledged while there was no control plane |
| Restore | on worker `m02` from its own copy of the bundle; the API server answered `/readyz` 64 s later |
| Kubernetes works again | a deleted shard-a Pod was replaced and Ready after 38 s; a new Job was scheduled and completed on `m03` |
| Databases | same primaries, epochs and synchronous standbys as before; all members streaming 43 s after the restore |
| Database checks | all 11 passed: 4001 request IDs, all succeeded with 0 client retries; 7176 ledger rows; 1576 COMMITTED transfers with matching participant rows; money 5350.00 + 45.74 = 5395.74 exactly |

The longest gap between two acknowledged operations during the whole run
was 4.3 s. Losing management did not interrupt the bank.

Management was restored on a surviving worker, which keeps running
workloads. Restoring on a new VM would use the same steps, but in this
setup Minikube's `node add` refuses to create machines once its original VM1
is gone. The first attempt of this experiment also tested a stand-alone
recovery: there the script stopped after destroying VM1, and
`k8s_multivm.py restore-control-plane --on banking-mv-m06` brought the API
back in 45 s, with every application and database Pod still running.

## Demonstration checklist

Run client and server VMs on a network and show normal balance, deposit, and
withdraw requests; concurrent withdrawals on one account; each error case;
the server log; process restart with the same database; and, for the bonus,
stop Server A while requests are active and show Server B promoting and
serving retries. For the partition demo, cut only the A-B link while both
servers can still reach the witness; show that the backup refuses writes,
restore the link, and show the primary synchronizing the backup before writes
resume. Then stop the primary, demonstrate witness-backed promotion, restart
the former primary, and show it syncing from the active node without taking
leadership. Record the required screen video from those VM runs.
