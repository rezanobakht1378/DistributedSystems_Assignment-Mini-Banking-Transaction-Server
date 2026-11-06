from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from fastapi import HTTPException, status

_ACCOUNT_ID_PATTERN = re.compile(r"A[0-9]{4}\Z")
_DEFAULT_MAP_PATH = Path(__file__).with_name("shard-map.json")


@dataclass(frozen=True)
class ShardRange:
    shard_id: str
    range_start: str
    range_end: str

    def contains(self, account: str) -> bool:
        return self.range_start <= account <= self.range_end


def load_shard_map(path: Path = _DEFAULT_MAP_PATH) -> tuple[ShardRange, ...]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Could not load shard map from {path}.") from error

    if not isinstance(data, dict) or data.get("version") != 1:
        raise ValueError("Shard map must be a version 1 object.")
    rows = data.get("shards")
    if not isinstance(rows, list) or not rows:
        raise ValueError("Shard map must contain at least one shard.")

    shards: list[ShardRange] = []
    shard_ids: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Shard map entries must be objects.")
        shard_id = row.get("id")
        range_start = row.get("range_start")
        range_end = row.get("range_end")
        if (
            not isinstance(shard_id, str)
            or not shard_id
            or shard_id in shard_ids
            or not isinstance(range_start, str)
            or not isinstance(range_end, str)
            or not _ACCOUNT_ID_PATTERN.fullmatch(range_start)
            or not _ACCOUNT_ID_PATTERN.fullmatch(range_end)
            or range_start > range_end
        ):
            raise ValueError("Shard map contains an invalid or duplicate range.")
        shard_ids.add(shard_id)
        shards.append(ShardRange(shard_id, range_start, range_end))

    shards.sort(key=lambda shard: shard.range_start)
    for previous, current in zip(shards, shards[1:]):
        if previous.range_end >= current.range_start:
            raise ValueError("Shard account ranges must not overlap.")
    return tuple(shards)


SHARD_MAP = load_shard_map()
_SHARDS_BY_ID = {shard.shard_id: shard for shard in SHARD_MAP}


def shard_for_account(account: str) -> str:
    if not isinstance(account, str) or not _ACCOUNT_ID_PATTERN.fullmatch(account):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "invalid_account",
                "message": "Account ID is outside the configured shard map.",
            },
        )
    for shard in SHARD_MAP:
        if shard.contains(account):
            return shard.shard_id
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail={
            "error": "invalid_account",
            "message": "Account ID is outside the configured shard map.",
        },
    )


def account_range_for_shard(shard_id: str) -> tuple[str, str]:
    shard = _SHARDS_BY_ID.get(shard_id)
    if shard is None:
        raise ValueError(f"Unknown shard ID: {shard_id!r}.")
    return shard.range_start, shard.range_end
