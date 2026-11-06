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
interruption/failover, same- and cross-shard transfers, Coordinator crash
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
