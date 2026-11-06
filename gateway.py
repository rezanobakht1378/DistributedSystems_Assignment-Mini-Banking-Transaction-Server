from __future__ import annotations

import asyncio
import logging
import os
import re
import uuid
from contextlib import asynccontextmanager
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse, Response
from sqlalchemy.exc import SQLAlchemyError

from models import MAX_BALANCE, CoordinatorStore, Operation
from shard_map import SHARD_MAP, shard_for_account

_REQUEST_ID_PATTERN = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z")
_PARTICIPANTS = ("source", "destination")


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
) -> FastAPI:
    if shard_replicas is None:
        shard_replicas = {
            shard.shard_id: _replicas(
                f"BANK_{shard.shard_id.upper().replace('-', '_')}_REPLICAS"
            )
            for shard in SHARD_MAP
        }
    if set(shard_replicas) != {shard.shard_id for shard in SHARD_MAP}:
        raise ValueError("Replica configuration must match the configured shard map.")
    if any(not urls for urls in shard_replicas.values()):
        raise ValueError("Each shard must have at least one application replica.")

    if coordinator_store is None:
        coordinator_database_url = os.getenv("BANK_COORDINATOR_DATABASE_URL")
        if coordinator_database_url:
            coordinator_store = CoordinatorStore(coordinator_database_url)

    replicas = {shard_id: tuple(urls) for shard_id, urls in shard_replicas.items()}
    coordinator = coordinator_store
    logger = logging.getLogger("banking_gateway")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.client = httpx.AsyncClient(timeout=3, transport=transport)
        app.state.next_replica = {shard_id: 0 for shard_id in replicas}
        app.state.replica_lock = asyncio.Lock()
        app.state.transaction_locks = {}
        app.state.transaction_locks_guard = asyncio.Lock()
        recovery_task: asyncio.Task[None] | None = None
        if coordinator is not None:
            await asyncio.to_thread(coordinator.initialize)
            recovery_task = asyncio.create_task(recover_transactions(app))
        try:
            yield
        finally:
            if recovery_task is not None:
                recovery_task.cancel()
                try:
                    await recovery_task
                except asyncio.CancelledError:
                    pass
            await app.state.client.aclose()

    app = FastAPI(title="Mini Banking Shard Gateway", lifespan=lifespan)

    async def next_candidates(shard_id: str) -> tuple[str, ...]:
        urls = replicas[shard_id]
        async with app.state.replica_lock:
            start = app.state.next_replica[shard_id]
            app.state.next_replica[shard_id] = (start + 1) % len(urls)
        return tuple(urls[(start + offset) % len(urls)] for offset in range(len(urls)))

    async def transaction_lock(transaction_id: str) -> asyncio.Lock:
        async with app.state.transaction_locks_guard:
            lock = app.state.transaction_locks.get(transaction_id)
            if lock is None:
                lock = asyncio.Lock()
                app.state.transaction_locks[transaction_id] = lock
            return lock

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
        request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        headers = {"X-Request-ID": request_id}
        for replica in await next_candidates(shard_id):
            try:
                health_response = await app.state.client.get(replica + "/health")
                if not health_response.is_success:
                    continue
                upstream_response = await app.state.client.request(
                    request.method,
                    replica + request.url.path,
                    params=request.query_params,
                    json=body,
                    headers=headers,
                )
            except httpx.RequestError:
                continue

            replica_name = httpx.URL(replica).host
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
                health_response = await app.state.client.get(replica + "/health")
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

    async def send_abort(
        transaction: dict[str, Any],
        client_ip: str,
    ) -> dict[str, Any]:
        if coordinator is None:
            raise RuntimeError("The transaction coordinator is not configured.")
        if transaction["state"] == "PREPARING":
            transaction = await asyncio.to_thread(
                coordinator.decide,
                transaction["transaction_id"],
                "ABORT",
            )
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
        operation_fingerprint = Operation(
            "transfer",
            source_account,
            amount,
            destination_account,
        ).fingerprint()
        lock = await transaction_lock(transaction_id)
        async with lock:
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
                transaction = await asyncio.to_thread(
                    coordinator.decide,
                    transaction_id,
                    decision,
                )
                if decision == "COMMIT":
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
                return transaction["response"]
            if transaction["state"] == "ABORT":
                transaction = await send_abort(transaction, client_ip)
            if transaction["state"] == "ABORTED":
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

    async def recover_transactions(app: FastAPI) -> None:
        if coordinator is None:
            return
        while True:
            try:
                pending = await asyncio.to_thread(coordinator.pending)
                for candidate in pending:
                    lock = await transaction_lock(candidate["transaction_id"])
                    async with lock:
                        transaction = await asyncio.to_thread(
                            coordinator.get,
                            candidate["transaction_id"],
                        )
                        if transaction is None:
                            continue
                        if transaction["state"] == "PREPARING":
                            transaction = await asyncio.to_thread(
                                coordinator.decide,
                                transaction["transaction_id"],
                                "ABORT",
                            )
                        if transaction["state"] == "COMMIT":
                            await finish_commit(transaction, "coordinator-recovery")
                        elif transaction["state"] == "ABORT":
                            await send_abort(transaction, "coordinator-recovery")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Coordinator recovery pass failed.")
            await asyncio.sleep(2)

    @app.get("/health")
    async def health() -> dict[str, str]:
        for shard_id in replicas:
            await healthy_replica(shard_id)
        return {"status": "success"}

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
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": "unknown_transaction",
                    "message": "No coordinated transaction exists for this ID.",
                },
            )
        return {
            "transaction_id": transaction["transaction_id"],
            "state": transaction["state"],
            "source_vote": transaction["source_vote"],
            "destination_vote": transaction["destination_vote"],
            "source_ack": transaction["source_ack"],
            "destination_ack": transaction["destination_ack"],
            "response": transaction["response"],
            "error": transaction["error_message"],
        }

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app = create_app()
    uvicorn.run(
        app,
        host=os.getenv("BANK_HOST", "0.0.0.0"),
        port=int(os.getenv("BANK_GATEWAY_PORT", "8000")),
    )
