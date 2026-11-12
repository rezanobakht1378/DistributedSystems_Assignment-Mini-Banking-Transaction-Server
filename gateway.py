from __future__ import annotations

import asyncio
import ipaddress
import logging
import os
import re
import socket
import time
import uuid
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from sqlalchemy.exc import SQLAlchemyError

from models import MAX_BALANCE, CoordinatorStore, Operation
from shard_map import SHARD_MAP, shard_for_account

_REQUEST_ID_PATTERN = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z")
_PARTICIPANTS = ("source", "destination")
_UNRESOLVED_STATES = ("PREPARING", "COMMIT", "ABORT")


def _shard_environment_name(shard_id: str) -> str:
    return shard_id.upper().replace("-", "_")


def _error_code(response: httpx.Response) -> str | None:
    try:
        body = response.json()
    except ValueError:
        return None
    return body.get("error") if isinstance(body, dict) else None


def _shard_services() -> dict[str, tuple[str, int]] | None:
    services: dict[str, tuple[str, int]] = {}
    for shard in SHARD_MAP:
        variable = f"BANK_{_shard_environment_name(shard.shard_id)}_SERVICE"
        value = os.getenv(variable, "").strip()
        if not value:
            continue
        host, separator, port = value.rpartition(":")
        if not separator or not host or not port.isdigit():
            raise ValueError(f"{variable} must have the form host:port.")
        services[shard.shard_id] = (host, int(port))
    if not services:
        return None
    if set(services) != {shard.shard_id for shard in SHARD_MAP}:
        raise ValueError("Set a discovery service for every shard in the shard map.")
    return services


def _url_for_address(address: str, port: int) -> str:
    if ipaddress.ip_address(address).version == 6:
        return f"http://[{address}]:{port}"
    return f"http://{address}:{port}"


def _replicas(environment_variable: str) -> list[str]:
    replicas = [
        value.strip().rstrip("/")
        for value in os.getenv(environment_variable, "").split(",")
        if value.strip()
    ]
    if not replicas:
        raise ValueError(f"{environment_variable} must list at least one replica.")
    if len(set(replicas)) != len(replicas):
        raise ValueError(f"{environment_variable} contains duplicate replica URLs.")
    for replica in replicas:
        url = httpx.URL(replica)
        if url.scheme not in {"http", "https"} or not url.host:
            raise ValueError(
                f"{environment_variable} contains an invalid HTTP URL."
            )
    return replicas


def create_app(
    shard_replicas: dict[str, list[str]] | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    coordinator_store: CoordinatorStore | None = None,
    shard_services: dict[str, tuple[str, int]] | None = None,
) -> FastAPI:
    if shard_replicas is None and shard_services is None:
        shard_services = _shard_services()
        if shard_services is None:
            shard_replicas = {
                shard.shard_id: _replicas(
                    f"BANK_{_shard_environment_name(shard.shard_id)}_REPLICAS"
                )
                for shard in SHARD_MAP
            }
    shard_ids = {shard.shard_id for shard in SHARD_MAP}
    if shard_services is not None and set(shard_services) != shard_ids:
        raise ValueError("Discovery configuration must match the configured shard map.")
    if shard_replicas is not None:
        if set(shard_replicas) != shard_ids:
            raise ValueError("Replica configuration must match the configured shard map.")
        if any(not urls for urls in shard_replicas.values()):
            raise ValueError("Each shard must have at least one application replica.")

    if coordinator_store is None:
        coordinator_database_url = os.getenv("BANK_COORDINATOR_DATABASE_URL")
        if coordinator_database_url:
            coordinator_store = CoordinatorStore(coordinator_database_url)

    replicas = {
        shard_id: tuple(urls) for shard_id, urls in (shard_replicas or {}).items()
    }
    services = shard_services
    coordinator = coordinator_store
    logger = logging.getLogger("banking_gateway")
    node_id = os.getenv("BANK_NODE_ID", socket.gethostname())
    upstream_timeout = float(os.getenv("BANK_UPSTREAM_TIMEOUT", "3"))
    health_timeout = min(1.0, upstream_timeout)
    discovery_ttl = float(os.getenv("BANK_DISCOVERY_CACHE_SECONDS", "1"))
    lock_wait_seconds = float(os.getenv("BANK_2PC_LOCK_WAIT_SECONDS", "10"))

    registry = CollectorRegistry()
    requests_total = Counter(
        "bank_gateway_requests_total",
        "Client requests handled by the gateway.",
        ["route", "shard", "status"],
        registry=registry,
    )
    request_latency = Histogram(
        "bank_gateway_request_duration_seconds",
        "Client request latency at the gateway.",
        ["route"],
        registry=registry,
    )
    upstream_failures = Counter(
        "bank_gateway_upstream_failures_total",
        "Shard replicas skipped by the gateway, by reason.",
        ["shard", "reason"],
        registry=registry,
    )
    discovered_replicas = Gauge(
        "bank_gateway_discovered_replicas",
        "Ready shard application replicas currently known to the gateway.",
        ["shard"],
        registry=registry,
    )
    coordinator_up = Gauge(
        "bank_coordinator_db_up",
        "1 when the coordinator database is reachable and writable.",
        registry=registry,
    )
    decisions_total = Counter(
        "bank_2pc_decisions_total",
        "Durable 2PC decisions written by this gateway replica.",
        ["decision"],
        registry=registry,
    )
    outcomes_total = Counter(
        "bank_2pc_client_outcomes_total",
        "Cross-shard transfer results returned to clients.",
        ["outcome"],
        registry=registry,
    )
    recovery_total = Counter(
        "bank_2pc_recovery_actions_total",
        "Transactions resumed by the recovery loop of this gateway replica.",
        ["action"],
        registry=registry,
    )
    transactions_by_state = Gauge(
        "bank_2pc_transactions",
        "Coordinated transfers in the coordinator log, by 2PC state.",
        ["state"],
        registry=registry,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Coordinator calls run in threads and a 2PC lock wait can hold one
        # for seconds; the default pool is sized from the node's CPU count.
        executor = ThreadPoolExecutor(
            max_workers=int(os.getenv("BANK_GATEWAY_THREADS", "64")),
            thread_name_prefix="gateway",
        )
        asyncio.get_running_loop().set_default_executor(executor)
        app.state.client = httpx.AsyncClient(
            timeout=upstream_timeout, transport=transport)
        app.state.next_replica = {shard_id: 0 for shard_id in shard_ids}
        app.state.replica_lock = asyncio.Lock()
        app.state.transaction_locks = {}
        app.state.transaction_locks_guard = asyncio.Lock()
        app.state.discovered = {}
        app.state.coordinator_ready = False
        background: list[asyncio.Task[None]] = []
        if coordinator is not None:
            try:
                await initialize_coordinator()
                app.state.coordinator_ready = True
            except SQLAlchemyError as error:
                logger.warning(
                    "coordinator_db_unavailable_at_startup node=%s error=%s",
                    node_id,
                    str(error).splitlines()[0],
                )
            background.append(asyncio.create_task(watch_coordinator(app)))
            background.append(asyncio.create_task(recover_transactions(app)))
        try:
            yield
        finally:
            for task in background:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            await app.state.client.aclose()
            executor.shutdown(wait=False, cancel_futures=True)

    app = FastAPI(title="Mini Banking Shard Gateway", lifespan=lifespan)

    @app.middleware("http")
    async def observe_requests(request: Request, call_next):
        started = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            response.headers["X-Gateway-Replica"] = node_id
            return response
        finally:
            route = getattr(request.scope.get("route"), "path", "unmatched")
            if route not in {"/health", "/livez", "/readyz", "/metrics"}:
                requests_total.labels(
                    route,
                    getattr(request.state, "shard", "none"),
                    str(status_code),
                ).inc()
                request_latency.labels(route).observe(time.perf_counter() - started)

    async def initialize_coordinator() -> None:
        await asyncio.to_thread(coordinator.initialize)
        if services is None:
            for shard_id, urls in replicas.items():
                for url in urls:
                    replica_id = httpx.URL(url).host or url
                    await asyncio.to_thread(
                        coordinator.ensure_replica,
                        shard_id,
                        replica_id,
                        url,
                    )

    async def watch_coordinator(app: FastAPI) -> None:
        was_up: bool | None = None
        while True:
            up = False
            try:
                if not app.state.coordinator_ready:
                    await initialize_coordinator()
                    app.state.coordinator_ready = True
                    logger.info("coordinator_initialized node=%s", node_id)
                up = (await asyncio.to_thread(coordinator.status))["writable"]
                counts = await asyncio.to_thread(coordinator.state_counts)
                for state in (*_UNRESOLVED_STATES, "COMMITTED", "ABORTED"):
                    transactions_by_state.labels(state).set(counts.get(state, 0))
            except asyncio.CancelledError:
                raise
            except Exception as error:
                up = False
                if was_up is not False:
                    logger.warning(
                        "coordinator_db_unavailable node=%s error=%s",
                        node_id,
                        str(error).splitlines()[0] if str(error) else type(error).__name__,
                    )
            if up != was_up:
                logger.warning("coordinator_db_state node=%s up=%s", node_id, up)
            was_up = up
            coordinator_up.set(1 if up else 0)
            await asyncio.sleep(2)

    async def discover(shard_id: str) -> list[str]:
        assert services is not None
        now = time.monotonic()
        cached = app.state.discovered.get(shard_id)
        if cached is not None and now - cached[0] < discovery_ttl:
            return cached[1]
        host, port = services[shard_id]
        try:
            addresses = await asyncio.get_running_loop().getaddrinfo(
                host, port, type=socket.SOCK_STREAM)
            urls = sorted({_url_for_address(info[4][0], port) for info in addresses})
        except (socket.gaierror, OSError):
            urls = []
        if cached is None or cached[1] != urls:
            logger.info(
                "discovery shard=%s service=%s replicas=%s",
                shard_id,
                host,
                ",".join(urls) or "none",
            )
        app.state.discovered[shard_id] = (now, urls)
        discovered_replicas.labels(shard_id).set(len(urls))
        return urls

    async def next_candidates(shard_id: str) -> tuple[str, ...]:
        if services is not None:
            urls = await discover(shard_id)
        else:
            urls = (
                await asyncio.to_thread(coordinator.replica_urls, shard_id)
                if coordinator is not None and app.state.coordinator_ready
                else []
            )
            if not urls:
                urls = list(replicas.get(shard_id, ()))
        if not urls:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "shard_unavailable",
                    "message": f"No ready replicas are registered for {shard_id}.",
                },
            )
        async with app.state.replica_lock:
            start = app.state.next_replica.get(shard_id, 0)
            app.state.next_replica[shard_id] = (start + 1) % len(urls)
        return tuple(urls[(start + offset) % len(urls)] for offset in range(len(urls)))

    async def local_transaction_lock(transaction_id: str) -> asyncio.Lock:
        async with app.state.transaction_locks_guard:
            lock = app.state.transaction_locks.get(transaction_id)
            if lock is None:
                lock = asyncio.Lock()
                app.state.transaction_locks[transaction_id] = lock
            return lock

    @asynccontextmanager
    async def owned_transaction(
        transaction_id: str,
        wait_seconds: float,
    ) -> AsyncIterator[bool]:
        """Serialize work on one 2PC transaction across all gateway replicas.

        Yields False when another task or gateway Pod currently owns it.
        """
        local_lock = await local_transaction_lock(transaction_id)
        if wait_seconds <= 0 and local_lock.locked():
            yield False
            return
        async with local_lock:
            handle = await asyncio.to_thread(
                coordinator.acquire_transaction_lock,
                transaction_id,
                wait_seconds,
            )
            if handle is None:
                yield False
                return
            try:
                yield True
            finally:
                coordinator.release_transaction_lock(handle)
        async with app.state.transaction_locks_guard:
            if not local_lock.locked():
                app.state.transaction_locks.pop(transaction_id, None)

    async def healthy_replica(shard_id: str) -> str:
        for replica in await next_candidates(shard_id):
            try:
                response = await app.state.client.get(replica + "/health")
            except httpx.RequestError:
                continue
            if response.is_success:
                return replica
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "shard_unavailable",
                "message": f"No healthy application replica is available for {shard_id}.",
            },
        )

    async def proxy(
        request: Request,
        shard_id: str,
        body: dict[str, Any] | None = None,
    ) -> Response:
        request.state.shard = shard_id
        request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        headers = {"X-Request-ID": request_id}
        for replica in await next_candidates(shard_id):
            try:
                health_response = await app.state.client.get(
                    replica + "/health", timeout=health_timeout)
            except httpx.TimeoutException:
                upstream_failures.labels(shard_id, "timeout").inc()
                continue
            except httpx.RequestError:
                upstream_failures.labels(shard_id, "connection_error").inc()
                continue
            if not health_response.is_success:
                upstream_failures.labels(shard_id, "unhealthy").inc()
                continue
            try:
                upstream_response = await app.state.client.request(
                    request.method,
                    replica + request.url.path,
                    params=request.query_params,
                    json=body,
                    headers=headers,
                )
            except (httpx.ConnectError, httpx.ConnectTimeout):
                upstream_failures.labels(shard_id, "connection_error").inc()
                continue
            except httpx.TimeoutException:
                # The replica is alive but slow and may still apply the
                # operation; sending it to another replica would only add load.
                upstream_failures.labels(shard_id, "timeout").inc()
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "error": "upstream_timeout",
                        "message": (
                            f"{shard_id} did not answer in time; the operation may "
                            "still complete. Retry with the same X-Request-ID."
                        ),
                    },
                )
            except httpx.RequestError:
                # The replica went away mid-request; the same X-Request-ID makes
                # the retry on another replica return the single result.
                upstream_failures.labels(shard_id, "connection_error").inc()
                continue
            if (
                upstream_response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
                and _error_code(upstream_response) == "server_busy"
            ):
                upstream_failures.labels(shard_id, "busy").inc()
                continue

            replica_name = upstream_response.headers.get(
                "x-served-by", httpx.URL(replica).host)
            logger.info(
                "forwarded request_id=%s shard=%s replica=%s status=%s",
                request_id,
                shard_id,
                replica_name,
                upstream_response.status_code,
            )
            response_headers = {}
            for name in ("content-type", "x-request-id"):
                value = upstream_response.headers.get(name)
                if value is not None:
                    response_headers[name] = value
            response_headers["X-Backend-Replica"] = replica_name
            return Response(
                content=upstream_response.content,
                status_code=upstream_response.status_code,
                headers=response_headers,
            )

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "shard_unavailable",
                "message": f"No healthy application replica is available for {shard_id}.",
            },
        )

    def participant_payload(
        transaction: dict[str, Any],
        client_ip: str,
    ) -> dict[str, str]:
        return {
            "transaction_id": transaction["transaction_id"],
            "source_account": transaction["source_account"],
            "destination_account": transaction["destination_account"],
            "amount": transaction["amount"],
            "client_ip": client_ip,
        }

    async def participant_request(
        shard_id: str,
        phase: str,
        payload: dict[str, str],
    ) -> httpx.Response:
        last_response: httpx.Response | None = None
        for replica in await next_candidates(shard_id):
            try:
                health_response = await app.state.client.get(
                    replica + "/health", timeout=health_timeout)
                if not health_response.is_success:
                    continue
                response = await app.state.client.post(
                    replica + f"/internal/2pc/{phase}",
                    json=payload,
                )
            except httpx.RequestError:
                continue
            logger.info(
                "participant request_id=%s shard=%s replica=%s phase=%s status=%s",
                payload["transaction_id"],
                shard_id,
                httpx.URL(replica).host,
                phase,
                response.status_code,
            )
            if response.status_code >= 500:
                last_response = response
                continue
            return response
        if last_response is not None:
            return last_response
        raise httpx.ConnectError(
            f"No healthy replica is available for {shard_id}."
        )

    async def record_decision(
        transaction_id: str,
        decision: str,
    ) -> dict[str, Any]:
        try:
            transaction = await asyncio.to_thread(
                coordinator.decide,
                transaction_id,
                decision,
            )
        except RuntimeError:
            # The decision row is the single source of truth: if another
            # replica already made a different durable decision (possible
            # only after losing the ownership lock), follow that decision.
            transaction = await asyncio.to_thread(coordinator.get, transaction_id)
            logger.warning(
                "2pc_decision_conflict transaction_id=%s wanted=%s durable_state=%s node=%s",
                transaction_id,
                decision,
                transaction["state"],
                node_id,
            )
            return transaction
        decisions_total.labels(decision).inc()
        logger.info(
            "2pc transaction_id=%s state=%s node=%s",
            transaction_id,
            decision,
            node_id,
        )
        return transaction

    async def send_abort(
        transaction: dict[str, Any],
        client_ip: str,
    ) -> dict[str, Any]:
        if coordinator is None:
            raise RuntimeError("The transaction coordinator is not configured.")
        if transaction["state"] == "PREPARING":
            transaction = await record_decision(
                transaction["transaction_id"],
                "ABORT",
            )
        if transaction["state"] in {"COMMIT", "COMMITTED"}:
            return transaction
        payload = participant_payload(transaction, client_ip)
        shards = {
            "source": shard_for_account(transaction["source_account"]),
            "destination": shard_for_account(transaction["destination_account"]),
        }
        for participant in _PARTICIPANTS:
            transaction = await asyncio.to_thread(
                coordinator.get,
                transaction["transaction_id"],
            )
            if transaction[f"{participant}_ack"]:
                continue
            try:
                response = await participant_request(
                    shards[participant],
                    "abort",
                    payload,
                )
                if not response.is_success:
                    continue
                transaction = await asyncio.to_thread(
                    coordinator.record_ack,
                    transaction["transaction_id"],
                    participant,
                )
            except (httpx.RequestError, SQLAlchemyError):
                continue
        transaction = await asyncio.to_thread(
            coordinator.get,
            transaction["transaction_id"],
        )
        if transaction["state"] != "ABORTED":
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "transaction_abort_pending",
                    "transaction_id": transaction["transaction_id"],
                    "message": "The durable ABORT decision is being delivered to participants.",
                },
            )
        return transaction

    async def finish_commit(
        transaction: dict[str, Any],
        client_ip: str,
    ) -> dict[str, Any]:
        if coordinator is None:
            raise RuntimeError("The transaction coordinator is not configured.")
        if transaction["state"] != "COMMIT" and transaction["state"] != "COMMITTED":
            raise RuntimeError("Cannot commit without a durable COMMIT decision.")
        payload = participant_payload(transaction, client_ip)
        shards = {
            "source": shard_for_account(transaction["source_account"]),
            "destination": shard_for_account(transaction["destination_account"]),
        }
        for participant in _PARTICIPANTS:
            transaction = await asyncio.to_thread(
                coordinator.get,
                transaction["transaction_id"],
            )
            if transaction[f"{participant}_ack"]:
                continue
            try:
                response = await participant_request(
                    shards[participant],
                    "commit",
                    payload,
                )
                if not response.is_success:
                    continue
                participant_result = response.json()
                if not isinstance(participant_result, dict):
                    continue
                if (
                    participant_result.get("transaction_id")
                    != transaction["transaction_id"]
                    or participant_result.get("role")
                    != ("debit" if participant == "source" else "credit")
                ):
                    continue
                transaction = await asyncio.to_thread(
                    coordinator.record_ack,
                    transaction["transaction_id"],
                    participant,
                    participant_result,
                )
            except (httpx.RequestError, ValueError, SQLAlchemyError):
                continue
        transaction = await asyncio.to_thread(
            coordinator.get,
            transaction["transaction_id"],
        )
        if transaction["state"] != "COMMITTED" or transaction["response"] is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "transaction_commit_pending",
                    "transaction_id": transaction["transaction_id"],
                    "decision": "COMMIT",
                    "message": (
                        "COMMIT is durable. Participants are being retried; "
                        "query the transaction status before retrying with a new ID."
                    ),
                },
            )
        return transaction

    async def coordinate_transfer(
        transaction_id: str,
        source_account: str,
        destination_account: str,
        amount: Decimal,
        client_ip: str,
    ) -> dict[str, Any]:
        if coordinator is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "coordinator_unavailable",
                    "message": "Cross-shard transfers require a persistent coordinator database.",
                },
            )
        if not app.state.coordinator_ready:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "coordinator_unavailable",
                    "message": "The coordinator database is not reachable yet.",
                },
            )
        operation_fingerprint = Operation(
            "transfer",
            source_account,
            amount,
            destination_account,
        ).fingerprint()
        async with owned_transaction(transaction_id, lock_wait_seconds) as owned:
            if not owned:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "error": "transaction_in_progress",
                        "transaction_id": transaction_id,
                        "message": (
                            "Another gateway replica is resolving this transaction; "
                            "query its status or retry with the same ID."
                        ),
                    },
                )
            transaction = await asyncio.to_thread(
                coordinator.begin,
                transaction_id,
                operation_fingerprint,
                source_account,
                destination_account,
                amount,
            )
            if transaction["state"] == "COMMITTED":
                return transaction["response"]
            if transaction["state"] == "ABORTED":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "transaction_aborted",
                        "transaction_id": transaction_id,
                        "message": transaction["error_message"] or "The transfer was aborted.",
                    },
                )

            if transaction["state"] == "PREPARING":
                shards = {
                    "source": shard_for_account(source_account),
                    "destination": shard_for_account(destination_account),
                }
                payload = participant_payload(transaction, client_ip)
                for participant in _PARTICIPANTS:
                    transaction = await asyncio.to_thread(
                        coordinator.get,
                        transaction_id,
                    )
                    if transaction[f"{participant}_vote"] is not None:
                        continue
                    try:
                        response = await participant_request(
                            shards[participant],
                            "prepare",
                            payload,
                        )
                        if response.is_success:
                            vote_body = response.json()
                            voted_yes = (
                                isinstance(vote_body, dict)
                                and vote_body.get("vote") == "YES"
                            )
                            reason = (
                                None if voted_yes else
                                str(vote_body.get("reason", "participant_voted_no"))
                                if isinstance(vote_body, dict)
                                else "invalid_vote"
                            )
                        else:
                            voted_yes = False
                            reason = f"participant_http_{response.status_code}"
                    except (httpx.RequestError, ValueError) as error:
                        voted_yes = False
                        reason = f"participant_unavailable: {error}"
                    transaction = await asyncio.to_thread(
                        coordinator.record_vote,
                        transaction_id,
                        participant,
                        voted_yes,
                        reason,
                    )
                    if not voted_yes:
                        break
                    if participant == "source":
                        pause_seconds = float(
                            os.getenv("BANK_EXPERIMENT_PAUSE_AFTER_PREPARE_SECONDS", "0"))
                        if pause_seconds > 0:
                            logger.warning(
                                "experiment failpoint: transaction_id=%s durable=PREPARING "
                                "source_vote=YES pause_seconds=%s",
                                transaction_id,
                                pause_seconds,
                            )
                            await asyncio.sleep(pause_seconds)

                transaction = await asyncio.to_thread(
                    coordinator.get,
                    transaction_id,
                )
                decision = (
                    "COMMIT"
                    if transaction["source_vote"] is True
                    and transaction["destination_vote"] is True
                    else "ABORT"
                )
                transaction = await record_decision(transaction_id, decision)
                if transaction["state"] == "COMMIT":
                    pause_seconds = float(
                        os.getenv(
                            "BANK_EXPERIMENT_PAUSE_AFTER_COMMIT_SECONDS",
                            "0",
                        )
                    )
                    if pause_seconds > 0:
                        logger.warning(
                            "experiment failpoint: transaction_id=%s durable=COMMIT pause_seconds=%s",
                            transaction_id,
                            pause_seconds,
                        )
                        await asyncio.sleep(pause_seconds)

            if transaction["state"] == "COMMIT":
                transaction = await finish_commit(transaction, client_ip)
            if transaction["state"] == "COMMITTED":
                outcomes_total.labels("committed").inc()
                logger.info(
                    "2pc transaction_id=%s state=COMMITTED node=%s",
                    transaction_id,
                    node_id,
                )
                return transaction["response"]
            if transaction["state"] == "ABORT":
                transaction = await send_abort(transaction, client_ip)
            if transaction["state"] == "ABORTED":
                outcomes_total.labels("aborted").inc()
                logger.info(
                    "2pc transaction_id=%s state=ABORTED node=%s reason=%s",
                    transaction_id,
                    node_id,
                    transaction["error_message"],
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "transaction_aborted",
                        "transaction_id": transaction_id,
                        "message": transaction["error_message"] or "A participant voted NO.",
                    },
                )
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "transaction_in_progress",
                    "transaction_id": transaction_id,
                    "message": "The transaction is still being resolved.",
                },
            )

    async def recover_transaction(transaction_id: str) -> None:
        async with owned_transaction(transaction_id, 0) as owned:
            if not owned:
                return
            transaction = await asyncio.to_thread(coordinator.get, transaction_id)
            if transaction is None or transaction["state"] not in _UNRESOLVED_STATES:
                return
            logger.warning(
                "2pc_recovery transaction_id=%s state=%s node=%s",
                transaction_id,
                transaction["state"],
                node_id,
            )
            if transaction["state"] == "PREPARING":
                recovery_total.labels("abort_orphaned_prepare").inc()
                transaction = await record_decision(transaction_id, "ABORT")
            if transaction["state"] == "COMMIT":
                recovery_total.labels("finish_commit").inc()
                await finish_commit(transaction, "coordinator-recovery")
            elif transaction["state"] == "ABORT":
                recovery_total.labels("finish_abort").inc()
                await send_abort(transaction, "coordinator-recovery")

    async def recover_transactions(app: FastAPI) -> None:
        if coordinator is None:
            return
        while True:
            if app.state.coordinator_ready:
                try:
                    pending = await asyncio.to_thread(coordinator.pending)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Coordinator recovery could not list transactions.")
                    pending = []
                for candidate in pending:
                    try:
                        await recover_transaction(candidate["transaction_id"])
                    except asyncio.CancelledError:
                        raise
                    except HTTPException as error:
                        logger.warning(
                            "2pc_recovery_pending transaction_id=%s detail=%s",
                            candidate["transaction_id"],
                            error.detail,
                        )
                    except Exception:
                        logger.exception(
                            "Coordinator recovery failed for %s.",
                            candidate["transaction_id"],
                        )
            await asyncio.sleep(2)

    @app.exception_handler(SQLAlchemyError)
    async def coordinator_error_handler(request: Request, error: SQLAlchemyError):
        logger.error(
            "coordinator_db_error node=%s error=%s",
            node_id,
            str(error).splitlines()[0] if str(error) else type(error).__name__,
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "error",
                "error": "coordinator_unavailable",
                "message": (
                    "The coordinator database is unavailable. Durable 2PC "
                    "decisions are kept; retry with the same X-Request-ID."
                ),
            },
        )

    @app.get("/health")
    async def health() -> dict[str, str]:
        for shard_id in shard_ids:
            await healthy_replica(shard_id)
        return {"status": "success"}

    @app.get("/livez")
    async def liveness() -> dict[str, str]:
        return {"status": "alive", "node_id": node_id}

    @app.get("/readyz")
    async def readiness():
        # Readiness is per gateway Pod, not per shard: one failed shard must
        # not remove the entry point for the healthy shards.
        if coordinator is not None and not app.state.coordinator_ready:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"status": "unavailable", "reason": "coordinator_not_initialized"},
            )
        return {"status": "ready", "node_id": node_id}

    @app.get("/status")
    async def cluster_status():
        shards: dict[str, Any] = {}
        for shard_id in sorted(shard_ids):
            try:
                candidates = await next_candidates(shard_id)
            except HTTPException:
                candidates = ()
            healthy = 0
            for replica in candidates:
                try:
                    response = await app.state.client.get(replica + "/health")
                    healthy += response.is_success
                except httpx.RequestError:
                    pass
            shards[shard_id] = {"replicas": len(candidates), "healthy": healthy}
        coordinator_state: dict[str, Any] = {"configured": coordinator is not None}
        if coordinator is not None:
            try:
                coordinator_state.update(await asyncio.to_thread(coordinator.status))
                coordinator_state["transactions"] = await asyncio.to_thread(
                    coordinator.state_counts)
            except SQLAlchemyError:
                coordinator_state.update({"reachable": False, "writable": False})
        return {"gateway": node_id, "shards": shards, "coordinator": coordinator_state}

    @app.get("/metrics")
    async def metrics():
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    @app.get("/balance/inquiry")
    async def balance(request: Request, account: str):
        return await proxy(request, shard_for_account(account))

    @app.post("/transactions")
    async def transact(request: Request):
        try:
            body = await request.json()
        except ValueError as error:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "invalid_request",
                    "message": "The request body must be valid JSON.",
                },
            ) from error
        if not isinstance(body, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "invalid_request",
                    "message": "The request body must be a JSON object.",
                },
            )
        account = body.get("account")
        if not isinstance(account, str) or not account:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "missing_field",
                    "message": "account is required.",
                },
            )
        shard_id = shard_for_account(account)
        if body.get("action") == "transfer":
            destination = body.get("destination")
            if not isinstance(destination, str) or not destination:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "missing_field",
                        "message": "destination is required for a transfer.",
                    },
                )
            destination_shard = shard_for_account(destination)
            if destination_shard != shard_id:
                try:
                    amount = Decimal(str(body.get("amount")))
                except (InvalidOperation, ValueError):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "invalid_amount",
                            "message": "amount must be a positive number with at most two decimal places.",
                        },
                    ) from None
                if (
                    not amount.is_finite()
                    or amount <= 0
                    or amount > MAX_BALANCE
                    or amount.as_tuple().exponent < -2
                ):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "invalid_amount",
                            "message": "amount must be a positive number with at most two decimal places.",
                        },
                    )
                request.state.shard = "cross-shard"
                request_id = request.headers.get("X-Request-ID", "")
                if not _REQUEST_ID_PATTERN.fullmatch(request_id):
                    request_id = uuid.uuid4().hex
                client_ip = request.client.host if request.client else "unknown"
                result = await coordinate_transfer(
                    request_id,
                    account,
                    destination,
                    amount,
                    client_ip,
                )
                return JSONResponse(
                    content=result,
                    headers={"X-Request-ID": request_id},
                )
        return await proxy(request, shard_id, body)

    @app.get("/transactions/{transaction_id}")
    async def transaction_status(transaction_id: str):
        if coordinator is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "coordinator_unavailable",
                    "message": "The transaction coordinator is not configured.",
                },
            )
        if not _REQUEST_ID_PATTERN.fullmatch(transaction_id):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "invalid_transaction_id",
                    "message": "Transaction ID has an invalid format.",
                },
            )
        transaction = await asyncio.to_thread(coordinator.get, transaction_id)
        if transaction is None:
            return {
                "transaction_id": transaction_id,
                "status": "UNKNOWN",
                "state": None,
                "message": "No coordinated transaction exists for this ID.",
            }
        state = transaction["state"]
        if state in {"PREPARING", "COMMIT", "ABORT"}:
            public_status = "PENDING"
        elif state == "COMMITTED":
            public_status = "COMPLETED"
        elif state == "ABORTED":
            public_status = "ABORTED"
        else:
            raise RuntimeError(
                f"Coordinator returned an unsupported transaction state: {state!r}."
            )
        return {
            "transaction_id": transaction["transaction_id"],
            "status": public_status,
            "state": state,
            "source_vote": transaction["source_vote"],
            "destination_vote": transaction["destination_vote"],
            "source_ack": transaction["source_ack"],
            "destination_ack": transaction["destination_ack"],
            "response": transaction["response"],
            "error": transaction["error_message"],
        }

    return app


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    app = create_app()
    uvicorn.run(
        app,
        host=os.getenv("BANK_HOST", "0.0.0.0"),
        port=int(os.getenv("BANK_GATEWAY_PORT", "8000")),
        timeout_graceful_shutdown=int(
            os.getenv("BANK_GRACEFUL_SHUTDOWN_SECONDS", "25")),
    )
