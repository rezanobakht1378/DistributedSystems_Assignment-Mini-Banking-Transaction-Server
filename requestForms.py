from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class RequestModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    @field_validator("amount", check_fields=False)
    @classmethod
    def validate_amount(cls, value: Decimal | None) -> Decimal | None:
        if value is None:
            return value
        if not value.is_finite() or value <= 0:
            raise ValueError("amount must be a positive finite number")
        if value.as_tuple().exponent < -2:
            raise ValueError(
                "amount must have no more than two decimal places")
        return value


class TransactionRequest(RequestModel):
    action: str
    account: str | None = None
    destination: str | None = None
    amount: Decimal | None = None


class ReplicationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: str
    account: str
    destination: str | None = None
    amount: Decimal = Field(gt=0)
    request_id: str = Field(min_length=1, max_length=128)
    client_ip: str = "unknown"
    leader_id: str = Field(min_length=1, max_length=128)
    epoch: int = Field(ge=1)

    @field_validator("action")
    @classmethod
    def validate_action(cls, value: str) -> str:
        if value not in {"deposit", "withdraw", "transfer"}:
            raise ValueError(
                "replicated action must be a balance-changing operation")
        return value

    @field_validator("amount")
    @classmethod
    def validate_replicated_amount(cls, value: Decimal) -> Decimal:
        if not value.is_finite() or value.as_tuple().exponent < -2:
            raise ValueError(
                "amount must be a finite number with at most two decimal places")
        return value

    @model_validator(mode="after")
    def validate_transfer_destination(self) -> ReplicationRequest:
        if self.action == "transfer" and not self.destination:
            raise ValueError("destination is required for a transfer")
        return self


class CrossShardParticipantRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transaction_id: str = Field(min_length=1, max_length=128)
    source_account: str
    destination_account: str
    amount: Decimal = Field(gt=0)
    client_ip: str = "unknown"

    @field_validator("amount")
    @classmethod
    def validate_transfer_amount(cls, value: Decimal) -> Decimal:
        if not value.is_finite() or value.as_tuple().exponent < -2:
            raise ValueError(
                "amount must be a finite number with at most two decimal places")
        return value
