from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from logging.handlers import RotatingFileHandler
from threading import BoundedSemaphore, Lock
from typing import Any

import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from models import (
    MAX_BALANCE,
    BankStore,
    BankWitness,
    Operation,
    ShardedBankStore,
)
from requestForms import (
    CrossShardParticipantRequest,
    ReplicationRequest,
    TransactionRequest,
)
from shard_map import account_range_for_shard, shard_for_account

load_dotenv()

_REQUEST_ID_PATTERN = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z")


@dataclass(frozen=True)
class Settings:
    database_url: str
    max_clients: int
    role: str
    node_id: str
    peer_url: str | None
    primary_url: str | None
    heartbeat_interval: float
    failure_threshold: int
    log_file: str
    witness_database_url: str | None = None
    cluster_id: str = "mini-banking"
    shard_a_database_url: str | None = None
    shard_b_database_url: str | None = None
    shard_id: str | None = None

    @classmethod
    def from_environment(cls) -> Settings:
        role = os.getenv("BANK_ROLE", "primary").strip().lower()
        peer_url = os.getenv("BANK_PEER_URL") or None
        primary_url = os.getenv("BANK_PRIMARY_URL") or None
        max_clients = int(os.getenv("BANK_MAX_CLIENTS", "100"))
        heartbeat_interval = float(os.getenv("BANK_HEARTBEAT_INTERVAL", "1"))
        failure_threshold = int(os.getenv("BANK_FAILURE_THRESHOLD", "3"))
        witness_database_url = os.getenv("BANK_WITNESS_DATABASE_URL") or None
        shard_a_database_url = os.getenv("BANK_SHARD_A_DATABASE_URL") or None
        shard_b_database_url = os.getenv("BANK_SHARD_B_DATABASE_URL") or None
        shard_id = os.getenv("BANK_SHARD_ID") or None
        if (shard_a_database_url is None) != (shard_b_database_url is None):
            raise ValueError(
                "Set both BANK_SHARD_A_DATABASE_URL and BANK_SHARD_B_DATABASE_URL."
            )
        if shard_id is not None:
            account_range_for_shard(shard_id)
            if shard_a_database_url is not None:
                raise ValueError(
                    "BANK_SHARD_ID cannot be combined with the dual-shard database URLs."
                )
        if role not in {"primary", "backup"}:
            raise ValueError("BANK_ROLE must be 'primary' or 'backup'.")
        if role == "backup" and not primary_url:
            raise ValueError(
                "BANK_PRIMARY_URL is required when BANK_ROLE=backup.")
        if (role == "backup" or peer_url or primary_url) and not witness_database_url:
            raise ValueError(
                "BANK_WITNESS_DATABASE_URL is required for primary/backup mode."
            )
        if max_clients < 1 or heartbeat_interval <= 0 or failure_threshold < 1:
            raise ValueError(
                "Client limit and failover timing settings must be positive.")
        database_url = os.getenv(
            "DATABASE_URL", "postgresql://localhost:5432/banking_db")
        return cls(
            database_url=database_url,
            max_clients=max_clients,
            role=role,
            node_id=os.getenv("BANK_NODE_ID", "server-a"),
            peer_url=peer_url.rstrip("/") if peer_url else None,
            primary_url=primary_url.rstrip("/") if primary_url else None,
            heartbeat_interval=heartbeat_interval,
            failure_threshold=failure_threshold,
            log_file=os.getenv("BANK_LOG_FILE", "banking_server.log"),
            witness_database_url=witness_database_url,
            cluster_id=os.getenv("BANK_CLUSTER_ID", "mini-banking"),
            shard_a_database_url=shard_a_database_url,
            shard_b_database_url=shard_b_database_url,
            shard_id=shard_id,
        )


def _configure_logger(log_file: str) -> logging.Logger:
    logger = logging.getLogger("banking_server")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
        formatter.converter = time.gmtime
        file_handler = RotatingFileHandler(
            log_file, maxBytes=50 * 1024 * 1024, backupCount=3, encoding="utf-8")
        stream_handler = logging.StreamHandler()
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
    return logger


def _validated_operation(
    action: str,
    account: str | None,
    amount: Decimal | None,
    destination: str | None = None,
) -> Operation:
    if action not in {"balance", "deposit", "withdraw", "transfer"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={
                            "error": "unsupported_action", "message": f"Action {action!r} is not supported."})
    if not account:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={
                            "error": "missing_field", "message": "account is required."})
    if action == "balance":
        return Operation(action="balance", account=account)
    if amount is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={
                            "error": "missing_field", "message": f"amount is required for {action}."})
    if amount <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={
                            "error": "invalid_amount", "message": "amount must be greater than zero."})
    if amount > MAX_BALANCE or amount.as_tuple().exponent < -2:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={
                            "error": "invalid_amount", "message": "amount must be within range and have no more than two decimal places."})
    amount = amount.quantize(Decimal("0.01"))
    if action == "transfer" and not destination:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "missing_field",
                "message": "destination is required for a transfer.",
            },
        )
    if action == "transfer" and destination == account:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "invalid_transfer",
                "message": "Source and destination accounts must differ.",
            },
        )
    return Operation(
        action,
        account,
        amount,
        destination if action == "transfer" else None,
    )


def _validate_shard_operation(
    shard_id: str | None,
    operation: Operation,
) -> None:
    if shard_id is None:
        return
    if shard_for_account(operation.account) != shard_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "account_not_on_shard",
                "message": f"Account {operation.account} is not owned by {shard_id}.",
            },
        )
    if (
        operation.action == "transfer"
        and operation.destination is not None
        and shard_for_account(operation.destination) != shard_id
    ):
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail={
                "error": "cross_shard_transfer_not_supported",
                "message": (
                    "Transfers between shards require the transaction "
                    "coordinator and 2PC, which are not configured."
                ),
            },
        )


def create_app(settings: Settings | None = None) -> FastAPI:
    config = settings or Settings.from_environment()
    logger = _configure_logger(config.log_file)
    if config.shard_a_database_url and config.shard_b_database_url:
        store = ShardedBankStore(
            config.shard_a_database_url,
            config.shard_b_database_url,
        )
    else:
        store = BankStore(config.database_url)
    witness = (
        BankWitness(config.witness_database_url, config.cluster_id, logger)
        if config.witness_database_url
        else None
    )
    if (config.role == "backup" or config.peer_url or config.primary_url) and witness is None:
        raise ValueError(
            "A PostgreSQL witness is required for primary/backup server configuration."
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if config.shard_id is None:
            store.initialize()
        else:
            store.initialize(account_range_for_shard(config.shard_id))
        monitor: asyncio.Task[None] | None = None
        if witness is not None:
            witness.initialize()
            if config.role == "primary":
                existing_leader = await asyncio.to_thread(witness.current_leader)
                if existing_leader is not None and config.peer_url:
                    app.state.role = "standby"
                    app.state.needs_sync = True
                    app.state.needs_reconciliation = True
                    app.state.can_promote = False
                    logger.warning(
                        "Node %s is waiting for its backup to confirm state before taking leadership.",
                        config.node_id,
                    )
                else:
                    try:
                        epoch = witness.try_acquire(
                            config.node_id,
                            store.replica_epoch(config.cluster_id),
                            allow_initial=True,
                        )
                        if epoch is not None:
                            store.set_replica_epoch(config.cluster_id, epoch)
                            app.state.cluster_epoch = epoch
                            app.state.role = "primary"
                            app.state.needs_sync = False
                        else:
                            app.state.role = "standby"
                    except SQLAlchemyError:
                        logger.exception(
                            "Could not acquire the PostgreSQL witness lock.")
                        app.state.role = "standby"
            else:
                app.state.needs_sync = store.replica_epoch(
                    config.cluster_id) is None
                app.state.needs_reconciliation = not app.state.needs_sync
            monitor = asyncio.create_task(_watch_cluster(app))
        try:
            yield
        finally:
            if monitor is not None:
                monitor.cancel()
                try:
                    await monitor
                except asyncio.CancelledError:
                    pass
            if witness is not None:
                witness.close()

    app = FastAPI(
        title="Reza Nobakht`s Mini Banking Transaction Server", lifespan=lifespan)
    app.state.settings = config
    app.state.store = store
    app.state.role = (
        "standby" if witness is not None and config.role == "primary" else config.role
    )
    app.state.peer_url = config.peer_url
    app.state.write_locked = False
    app.state.needs_sync = witness is not None
    app.state.needs_reconciliation = False
    app.state.can_promote = True
    app.state.witness = witness
    app.state.cluster_epoch = None
    app.state.write_lock = Lock()
    app.state.logger = logger
    request_slots = BoundedSemaphore(config.max_clients)

    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        supplied_id = request.headers.get("X-Request-ID", "")
        request_id = supplied_id if _REQUEST_ID_PATTERN.fullmatch(
            supplied_id) else uuid.uuid4().hex
        request.state.request_id = request_id
        request.state.account_id = request.query_params.get("account", "N/A")
        client_ip = request.client.host if request.client else "unknown"
        started = time.perf_counter()
        acquired = request_slots.acquire(blocking=False)
        try:
            if acquired:
                try:
                    response = await call_next(request)
                except Exception:
                    result = "error"
                    raise
                result = "success" if response.status_code < 400 else f"http_{response.status_code}"
            else:
                response = JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={
                                        "status": "error", "error": "server_busy", "message": "Maximum concurrent request capacity is reached."})
                result = "http_503"
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            if acquired:
                request_slots.release()
            latency_ms = (time.perf_counter() - started) * 1000
            logger.info(
                "request_id=%r client_ip=%r action=%r account=%r result=%r latency_ms=%.2f",
                request_id,
                client_ip,
                getattr(request.state, "action",
                        request.method + " " + request.url.path),
                getattr(request.state, "account_id", "N/A"),
                result,
                latency_ms,
            )

    @app.exception_handler(HTTPException)
    async def http_error_handler(request: Request, error: HTTPException):
        if isinstance(error.detail, dict):
            content = {"status": "error", **error.detail}
        else:
            content = {"status": "error", "error": "request_error",
                       "message": str(error.detail)}
        response = JSONResponse(status_code=error.status_code, content=content)
        request_id = getattr(request.state, "request_id", None)
        if request_id:
            response.headers["X-Request-ID"] = request_id
        return response

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, error: RequestValidationError):
        if isinstance(error.body, dict):
            request.state.account_id = error.body.get("account", "N/A")
        errors = [
            {
                "field": ".".join(str(part) for part in item["loc"][1:]),
                "message": item["msg"],
            }
            for item in error.errors()
        ]
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "status": "error",
                "error": "invalid_request",
                "message": "The request is malformed or contains invalid fields.",
                "details": errors,
            },
        )

    def execute_operation(request: Request, operation: Operation) -> dict[str, Any]:
        request.state.account_id = operation.account
        _validate_shard_operation(config.shard_id, operation)
        if request.app.state.role != "primary":
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "standby",
                    "message": "This server is not currently the active primary.",
                },
            )
        if witness is not None and request.app.state.needs_sync:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "state_not_synchronized",
                    "message": "This server must synchronize before serving requests.",
                },
            )
        if witness is not None and not witness.owns_lock():
            request.app.state.role = "standby"
            request.app.state.peer_url = None
            request.app.state.write_locked = True
            request.app.state.needs_sync = True
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "witness_unavailable",
                    "message": "This server cannot verify leadership and has stopped serving requests.",
                },
            )
        if request.app.state.write_locked:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "replication_uncertain",
                    "message": "Writes are paused because the replica could not be confirmed. Use the active backup.",
                },
            )

        if operation.action == "balance":
            result = store.get_balance(operation.account)
            if witness is not None and not witness.owns_lock():
                request.app.state.role = "standby"
                request.app.state.peer_url = None
                request.app.state.write_locked = True
                request.app.state.needs_sync = True
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "error": "witness_unavailable",
                        "message": "Leadership changed before the balance could be confirmed.",
                    },
                )
            return result

        request_id = request.state.request_id
        client_ip = request.client.host if request.client else "unknown"
        with request.app.state.write_lock:
            replica_response: dict[str, Any] | None = None
            peer_url = request.app.state.peer_url
            if peer_url:
                try:
                    response = httpx.post(
                        peer_url + "/internal/replicate",
                        json={
                            "action": operation.action,
                            "account": operation.account,
                            "destination": operation.destination,
                            "amount": str(operation.amount),
                            "request_id": request_id,
                            "client_ip": client_ip,
                            "leader_id": config.node_id,
                            "epoch": request.app.state.cluster_epoch,
                        },
                        headers={"X-Request-ID": request_id},
                        timeout=3
                    )
                except httpx.RequestError as error:
                    request.app.state.write_locked = True
                    logger.error(
                        "Replica unavailable; writes locked: %s", error)
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail={
                            "error": "replica_unavailable",
                            "message": "The replica did not confirm the operation; writes are paused for safety.",
                        },
                    ) from error

                if response.status_code >= 500:
                    try:
                        error_body = response.json()
                    except ValueError:
                        error_body = {}
                    if (
                        isinstance(error_body, dict)
                        and error_body.get("error") == "replica_not_ready"
                    ):
                        raise HTTPException(
                            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail={
                                "error": "replica_not_ready",
                                "message": "The backup is synchronizing and cannot accept writes yet.",
                            },
                        )
                    request.app.state.write_locked = True
                    logger.error(
                        "Replica returned HTTP %s; writes locked.", response.status_code)
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail={
                            "error": "replication_uncertain",
                            "message": "The replica could not confirm the operation; writes are paused for safety.",
                        },
                    )
                if response.status_code >= 400:
                    try:
                        body = response.json()
                    except ValueError as error:
                        raise HTTPException(
                            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail={
                                "error": "replication_rejected",
                                "message": "The replica returned an invalid response.",
                            },
                        ) from error
                    if not isinstance(body, dict):
                        raise HTTPException(
                            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail={
                                "error": "replication_rejected",
                                "message": "The replica returned an invalid response.",
                            },
                        )
                    if response.status_code == 409 and body.get("error") == "not_standby":
                        request.app.state.role = "standby"
                        request.app.state.peer_url = None
                        request.app.state.write_locked = True
                        request.app.state.needs_sync = True
                        raise HTTPException(
                            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail={
                                "error": "role_changed",
                                "message": "This server has been superseded by the active backup.",
                            },
                        )
                    raise HTTPException(
                        status_code=response.status_code,
                        detail={
                            "error": body.get("error", "replication_rejected"),
                            "message": body.get("message", "The replica rejected the operation."),
                        },
                    )
                try:
                    replica_response = response.json()
                except ValueError as error:
                    request.app.state.write_locked = True
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail={
                            "error": "replication_uncertain",
                            "message": "The replica response could not be verified; writes are paused.",
                        },
                    ) from error

            if witness is not None and not witness.owns_lock():
                request.app.state.role = "standby"
                request.app.state.peer_url = None
                request.app.state.write_locked = True
                request.app.state.needs_sync = True
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "error": "witness_unavailable",
                        "message": "Leadership changed before the operation could commit.",
                    },
                )

            try:
                local_response = store.execute(
                    operation,
                    request_id,
                    client_ip,
                    config.cluster_id if witness is not None else None,
                    request.app.state.cluster_epoch,
                )
            except SQLAlchemyError as error:
                if replica_response is not None:
                    request.app.state.write_locked = True
                    logger.exception(
                        "Replica committed but local commit failed; writes locked.")
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail={
                            "error": "replication_uncertain",
                            "message": "The operation reached the replica but local state could not be confirmed.",
                        },
                    ) from error
                raise

            if replica_response is not None and replica_response != local_response:
                request.app.state.write_locked = True
                logger.error(
                    "Primary and replica responses differ; writes locked.")
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "error": "replica_divergence",
                        "message": "The replica and primary disagree; writes are paused for safety.",
                    },
                )
            if witness is not None and not witness.owns_lock():
                request.app.state.role = "standby"
                request.app.state.peer_url = None
                request.app.state.write_locked = True
                request.app.state.needs_sync = True
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "error": "witness_unavailable",
                        "message": "Leadership changed before the transaction could be confirmed.",
                    },
                )
            return local_response

    @app.get("/health")
    def health(request: Request):
        owns_leadership = witness is None or witness.owns_lock()
        if not owns_leadership and request.app.state.role == "primary":
            request.app.state.role = "standby"
            request.app.state.peer_url = None
            request.app.state.write_locked = True
            request.app.state.needs_sync = True
        if (
            request.app.state.role != "primary"
            or request.app.state.write_locked
            or request.app.state.needs_sync
            or not owns_leadership
        ):
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "status": "unavailable",
                    "role": request.app.state.role,
                    "node_id": config.node_id,
                },
            )
        return {
            "status": "success",
            "role": request.app.state.role,
            "node_id": config.node_id,
            "cluster_id": config.cluster_id if witness is not None else None,
            "epoch": request.app.state.cluster_epoch,
        }

    @app.get("/internal/snapshot")
    def snapshot(request: Request):
        owns_leadership = witness is None or witness.owns_lock()
        is_active_primary = (
            request.app.state.role == "primary"
            and owns_leadership
        )
        if not is_active_primary:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "not_active_primary",
                    "message": "Only the active primary can provide a state snapshot.",
                },
            )
        with request.app.state.write_lock:
            if witness is not None and not witness.owns_lock():
                request.app.state.role = "standby"
                request.app.state.peer_url = None
                request.app.state.write_locked = True
                request.app.state.needs_sync = True
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "error": "not_active_primary",
                        "message": "This node no longer holds the cluster leadership lock.",
                    },
                )
            return {
                "cluster_id": config.cluster_id if witness is not None else None,
                "node_id": config.node_id,
                "epoch": request.app.state.cluster_epoch,
                "state": store.export_snapshot(),
            }

    @app.post("/internal/snapshot")
    def restore_snapshot(payload: dict[str, Any], request: Request):
        sender = payload.get("node_id")
        epoch = payload.get("epoch")
        state = payload.get("state")
        if (
            payload.get("cluster_id") != config.cluster_id
            or not isinstance(sender, str)
            or type(epoch) is not int
            or not isinstance(state, dict)
            or witness is None
            or not witness.is_current_leader(sender, epoch)
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "invalid_snapshot_source",
                    "message": "Snapshot is not from the current cluster leader.",
                },
            )
        with request.app.state.write_lock:
            if (
                request.app.state.role == "primary"
                or witness.owns_lock()
                or not witness.is_current_leader(sender, epoch)
            ):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "snapshot_rejected",
                        "message": "Only a non-leader replica may restore cluster state.",
                    },
                )
            store.restore_snapshot(state, config.cluster_id, epoch)
            request.app.state.cluster_epoch = epoch
            request.app.state.needs_sync = False
            request.app.state.needs_reconciliation = False
        return {"status": "success", "epoch": epoch}

    @app.get("/balance/inquiry")
    def read_balance(
        request: Request,
        account: str,
    ):
        request.state.action = "balance"
        operation = _validated_operation("balance", account, None)
        return execute_operation(request, operation)

    @app.post("/transactions")
    def transact(body: TransactionRequest, request: Request):
        request.state.account_id = body.account or "N/A"
        request.state.action = body.action
        operation = _validated_operation(
            body.action,
            body.account,
            body.amount,
            body.destination,
        )
        return execute_operation(request, operation)

    @app.post("/internal/replicate")
    def replicate(body: ReplicationRequest, request: Request):
        request.state.account_id = body.account
        request.state.action = body.action
        if request.app.state.role == "primary":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "not_standby",
                    "message": "This server is not a standby replica.",
                },
            )
        if request.app.state.needs_sync:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "replica_not_ready",
                    "message": "The standby must synchronize before accepting writes.",
                },
            )
        if witness is None or not witness.is_current_leader(body.leader_id, body.epoch):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "stale_primary",
                    "message": "The request was not sent by the current cluster leader.",
                },
            )
        replica_epoch = store.replica_epoch(config.cluster_id)
        if replica_epoch is not None and body.epoch < replica_epoch:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "replica_not_ready",
                    "message": "The standby must synchronize to the current leader term.",
                },
            )
        operation = Operation(
            action=body.action,
            account=body.account,
            amount=body.amount,
            destination=(
                body.destination if body.action == "transfer" else None
            ),
        )
        _validate_shard_operation(config.shard_id, operation)
        with request.app.state.write_lock:
            if request.app.state.role == "primary":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "not_standby",
                        "message": "This server has become the active primary.",
                    },
                )
            if request.app.state.needs_sync:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "error": "replica_not_ready",
                        "message": "The standby must synchronize before accepting writes.",
                    },
                )
            if not witness.is_current_leader(body.leader_id, body.epoch):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "stale_primary",
                        "message": "The request was not sent by the current cluster leader.",
                    },
                )
            return store.execute(
                operation,
                body.request_id,
                body.client_ip,
                config.cluster_id,
                body.epoch,
            )

    def cross_shard_participant(
        body: CrossShardParticipantRequest,
    ) -> tuple[BankStore, str]:
        if config.shard_id is None or not isinstance(store, BankStore):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": "not_a_shard_participant",
                    "message": "This app is not configured as a shard participant.",
                },
            )
        source_shard = shard_for_account(body.source_account)
        destination_shard = shard_for_account(body.destination_account)
        if source_shard == destination_shard:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "not_cross_shard",
                    "message": "2PC participant endpoints require two different shards.",
                },
            )
        if config.shard_id == source_shard:
            return store, "debit"
        if config.shard_id == destination_shard:
            return store, "credit"
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "wrong_shard",
                "message": "This app does not own either account in the transfer.",
            },
        )

    @app.post("/internal/2pc/prepare")
    def prepare_cross_shard_transfer(body: CrossShardParticipantRequest):
        participant_store, role = cross_shard_participant(body)
        return participant_store.prepare_cross_shard_transfer(
            body.transaction_id,
            body.source_account,
            body.destination_account,
            body.amount,
            role,
            body.client_ip,
        )

    @app.post("/internal/2pc/commit")
    def commit_cross_shard_transfer(body: CrossShardParticipantRequest):
        participant_store, _ = cross_shard_participant(body)
        return participant_store.commit_cross_shard_transfer(
            body.transaction_id
        )

    @app.post("/internal/2pc/abort")
    def abort_cross_shard_transfer(body: CrossShardParticipantRequest):
        participant_store, role = cross_shard_participant(body)
        return participant_store.abort_cross_shard_transfer(
            body.transaction_id,
            body.source_account,
            body.destination_account,
            body.amount,
            role,
            body.client_ip,
        )

    @app.get("/internal/2pc/{transaction_id}")
    def cross_shard_transfer_status(
        transaction_id: str,
        request: Request,
    ):
        if config.shard_id is None or not isinstance(store, BankStore):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": "not_a_shard_participant",
                    "message": "This app is not configured as a shard participant.",
                },
            )
        request.state.action = "2pc_status"
        return store.cross_shard_transfer_status(transaction_id)

    return app


async def _watch_cluster(app: FastAPI) -> None:
    failures = 0
    config: Settings = app.state.settings
    witness: BankWitness = app.state.witness
    peer_url = config.primary_url or config.peer_url
    async with httpx.AsyncClient(timeout=config.heartbeat_interval) as client:
        while True:
            if app.state.role == "primary":
                owns_lock = await asyncio.to_thread(witness.owns_lock)
                if not owns_lock:
                    app.state.role = "standby"
                    app.state.peer_url = None
                    app.state.write_locked = True
                    app.state.needs_sync = True
                    app.state.needs_reconciliation = False
                    failures = 0
                    app.state.logger.error(
                        "Node %s lost its witness lock and is now fenced.",
                        config.node_id,
                    )
                elif app.state.write_locked and config.peer_url:
                    try:
                        await _acquire_thread_lock(app.state.write_lock)
                        try:
                            if app.state.write_locked and app.state.role == "primary":
                                payload = {
                                    "cluster_id": config.cluster_id,
                                    "node_id": config.node_id,
                                    "epoch": app.state.cluster_epoch,
                                    "state": app.state.store.export_snapshot(),
                                }
                                response = await client.post(
                                    config.peer_url + "/internal/snapshot",
                                    json=payload,
                                )
                                response.raise_for_status()
                                if await asyncio.to_thread(witness.owns_lock):
                                    app.state.write_locked = False
                                    app.state.logger.info(
                                        "Node %s synchronized standby %s and resumed writes.",
                                        config.node_id,
                                        config.peer_url,
                                    )
                        finally:
                            app.state.write_lock.release()
                    except (
                        httpx.RequestError,
                        httpx.HTTPStatusError,
                        ValueError,
                        SQLAlchemyError,
                    ):
                        app.state.logger.warning(
                            "Node %s is waiting to synchronize standby %s.",
                            config.node_id,
                            config.peer_url,
                            exc_info=True,
                        )

            if app.state.role != "primary":
                local_epoch = await asyncio.to_thread(
                    app.state.store.replica_epoch,
                    config.cluster_id,
                )
                if app.state.needs_sync and await asyncio.to_thread(
                    witness.current_leader
                ) == (config.node_id, local_epoch):
                    app.state.needs_sync = False

                peer_is_primary = False
                peer_epoch: int | None = None
                if peer_url:
                    try:
                        response = await client.get(peer_url + "/health")
                        health_body = response.json()
                        peer_is_primary = response.status_code == 200 and (
                            isinstance(health_body, dict)
                            and health_body.get("role") == "primary"
                            and health_body.get("cluster_id") == config.cluster_id
                            and type(health_body.get("epoch")) is int
                            and witness.is_current_leader(
                                health_body.get("node_id", ""),
                                health_body.get("epoch", 0),
                            )
                        )
                        if peer_is_primary:
                            peer_epoch = health_body["epoch"]
                    except (httpx.RequestError, ValueError):
                        peer_is_primary = False

                if peer_is_primary and peer_url is not None:
                    failures = 0
                    if (
                        app.state.needs_sync
                        or app.state.needs_reconciliation
                        or local_epoch != peer_epoch
                    ):
                        try:
                            response = await client.get(peer_url + "/internal/snapshot")
                            response.raise_for_status()
                            snapshot_data = response.json()
                            if (
                                not isinstance(snapshot_data, dict)
                                or snapshot_data.get("cluster_id") != config.cluster_id
                                or snapshot_data.get("epoch") != peer_epoch
                                or type(snapshot_data.get("epoch")) is not int
                                or not isinstance(snapshot_data.get("state"), dict)
                                or not witness.is_current_leader(
                                    snapshot_data.get("node_id", ""),
                                    peer_epoch,
                                )
                                or (
                                    local_epoch is not None
                                    and peer_epoch is not None
                                    and peer_epoch < local_epoch
                                )
                            ):
                                raise ValueError(
                                    "Primary returned an invalid state snapshot.")
                            await _acquire_thread_lock(app.state.write_lock)
                            try:
                                if (
                                    app.state.role != "primary"
                                    and not await asyncio.to_thread(witness.owns_lock)
                                ):
                                    await asyncio.to_thread(
                                        app.state.store.restore_snapshot,
                                        snapshot_data["state"],
                                        config.cluster_id,
                                        peer_epoch,
                                    )
                                    app.state.needs_sync = False
                                    app.state.needs_reconciliation = False
                                    app.state.cluster_epoch = peer_epoch
                                    app.state.write_locked = False
                                    app.state.can_promote = True
                                    app.state.logger.info(
                                        "Node %s synchronized state from %s.",
                                        config.node_id,
                                        peer_url,
                                    )
                            finally:
                                app.state.write_lock.release()
                        except (
                            httpx.RequestError,
                            httpx.HTTPStatusError,
                            ValueError,
                            KeyError,
                            TypeError,
                            SQLAlchemyError,
                        ):
                            app.state.logger.exception(
                                "Node %s could not synchronize from %s.",
                                config.node_id,
                                peer_url,
                            )
                else:
                    failures += 1

                if (
                    failures >= config.failure_threshold
                    and not app.state.needs_sync
                    and app.state.can_promote
                ):
                    try:
                        epoch = await asyncio.to_thread(
                            witness.try_acquire,
                            config.node_id,
                            local_epoch,
                            config.role == "primary",
                        )
                    except SQLAlchemyError:
                        app.state.logger.exception(
                            "Node %s could not contact the PostgreSQL witness.",
                            config.node_id,
                        )
                        epoch = None
                    if epoch is not None:
                        try:
                            await asyncio.to_thread(
                                app.state.store.set_replica_epoch,
                                config.cluster_id,
                                epoch,
                            )
                        except SQLAlchemyError:
                            app.state.logger.exception(
                                "Node %s could not persist its new cluster term.",
                                config.node_id,
                            )
                        else:
                            app.state.cluster_epoch = epoch
                            app.state.role = "primary"
                            app.state.peer_url = None
                            app.state.write_locked = False
                            app.state.needs_sync = False
                            app.state.needs_reconciliation = False
                            failures = 0
                            app.state.logger.warning(
                                "Node %s acquired witness term %d and became primary.",
                                config.node_id,
                                epoch,
                            )

            await asyncio.sleep(config.heartbeat_interval)


async def _acquire_thread_lock(lock: Lock) -> None:
    acquisition = asyncio.create_task(asyncio.to_thread(lock.acquire))
    try:
        await asyncio.shield(acquisition)
    except asyncio.CancelledError:
        await acquisition
        lock.release()
        raise


settings = Settings.from_environment()
app = create_app(settings)


if __name__ == "__main__":
    uvicorn.run(app, host=os.getenv("BANK_HOST", "0.0.0.0"),
                port=int(os.getenv("BANK_PORT", "8000")))
