from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit

import httpx


def _gateway_url(value: str | None) -> str:
    url = (value or "").strip().rstrip("/")
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise argparse.ArgumentTypeError(
            "Set BANK_GATEWAY_URL or pass --gateway with the gateway URL.")
    if "," in url:
        raise argparse.ArgumentTypeError(
            "Configure exactly one gateway URL; the gateway handles shard routing and replica failover.")
    return url


def _send(
    gateway_url: str,
    method: str,
    path: str,
    *,
    body: dict[str, Any] | None = None,
    params: dict[str, str] | None = None,
    request_id: str | None = None,
) -> httpx.Response:
    last_error: Exception | None = None
    headers = {"X-Request-ID": request_id} if request_id else {}
    with httpx.Client(timeout=2) as client:
        for attempt in range(12):
            try:
                response = client.request(
                    method,
                    gateway_url + path,
                    json=body,
                    params=params,
                    headers=headers,
                )
                if response.status_code < 500 and response.status_code != 503:
                    return response
                last_error = httpx.HTTPStatusError(
                    f"Gateway returned HTTP {response.status_code}.",
                    request=response.request,
                    response=response,
                )
            except httpx.RequestError as error:
                last_error = error
            if attempt < 11:
                time.sleep(0.5)
    if last_error is not None:
        raise last_error
    raise RuntimeError("No server URL was available.")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Client for the mini banking gateway.")
    parser.add_argument(
        "--gateway",
        default=os.getenv("BANK_GATEWAY_URL"),
        help="Gateway URL; defaults to BANK_GATEWAY_URL.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    balance = commands.add_parser("balance", help="Read an account balance.")
    balance.add_argument("account")

    for action in ("deposit", "withdraw"):
        command = commands.add_parser(
            action, help=f"{action.title()} to an account.")
        command.add_argument("account")
        command.add_argument("amount")

    transfer = commands.add_parser(
        "transfer", help="Transfer between simulated accounts.")
    transfer.add_argument("account", help="Source account.")
    transfer.add_argument("target_account", help="Destination account.")
    transfer.add_argument("amount")

    args = parser.parse_args(argv)
    try:
        gateway_url = _gateway_url(args.gateway)
        if args.command == "balance":
            response = _send(gateway_url, "GET", "/balance/inquiry",
                             params={"account": args.account})
        else:
            payload: dict[str, Any] = {
                "action": args.command,
                "account": args.account,
                "amount": args.amount,
            }
            if args.command == "transfer":
                payload["destination"] = args.target_account
            response = _send(gateway_url, "POST", "/transactions",
                             body=payload, request_id=uuid.uuid4().hex)
        try:
            payload = response.json()
        except ValueError:
            payload = {"status": "error", "message": response.text}
        print(json.dumps(payload, indent=2))
        return 0 if response.is_success else 1
    except (httpx.HTTPError, RuntimeError, argparse.ArgumentTypeError) as error:
        print(f"Request failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
