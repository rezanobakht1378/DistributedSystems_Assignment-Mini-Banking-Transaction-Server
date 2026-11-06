from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from fastapi import HTTPException, status
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    delete,
    func,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection, Engine, make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from threading import Lock

from shard_map import account_range_for_shard, shard_for_account

MAX_BALANCE = Decimal("999999999999999999.99")


def _postgres_connection_options(
    database_url: str,
    *required_options: str,
) -> str:
    configured_options = make_url(database_url).query.get("options", ())
    if isinstance(configured_options, str):
        configured_options = (configured_options,)
    return " ".join((*configured_options, *required_options))


class Base(DeclarativeBase):
    pass


class WitnessBase(DeclarativeBase):
    pass


class WitnessLeader(WitnessBase):
    __tablename__ = "bank_witness_leaders"

    cluster_id: Mapped[str] = mapped_column(String, primary_key=True)
    node_id: Mapped[str] = mapped_column(String, nullable=False)
    epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)


class Account(Base):
    __tablename__ = "accounts"
    __table_args__ = (CheckConstraint(
        "balance >= 0", name="accounts_balance_nonnegative"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    wallet: Mapped[str] = mapped_column(
        String, unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    balance: Mapped[Decimal] = mapped_column(
        Numeric(20, 2), nullable=False, default=Decimal("0"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class BankTransaction(Base):
    __tablename__ = "bank_transactions"
    __table_args__ = (
        CheckConstraint("entry IN ('in', 'out')",
                        name="bank_transactions_entry_valid"),
        CheckConstraint(
            "amount > 0", name="bank_transactions_amount_positive"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    request_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    action: Mapped[str] = mapped_column(String, nullable=False)
    account: Mapped[str] = mapped_column(String, nullable=False)
    entry: Mapped[str] = mapped_column(String, nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False)
    balance_before: Mapped[Decimal] = mapped_column(
        Numeric(20, 2), nullable=False)
    balance_after: Mapped[Decimal] = mapped_column(
        Numeric(20, 2), nullable=False)
    client_ip: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(
        timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class BankOperation(Base):
    __tablename__ = "bank_operations"

    request_id: Mapped[str] = mapped_column(String, primary_key=True)
    fingerprint: Mapped[str] = mapped_column(Text, nullable=False)
    response_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(
        timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class BankReplicaState(Base):
    __tablename__ = "bank_replica_state"

    cluster_id: Mapped[str] = mapped_column(String, primary_key=True)
    synchronized_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)


class PreparedTransfer(Base):
    __tablename__ = "bank_prepared_transfers"
    __table_args__ = (
        CheckConstraint(
            "role IN ('debit', 'credit')",
            name="bank_prepared_transfers_role_valid",
        ),
        CheckConstraint(
            "state IN ('prepared', 'committed', 'aborted')",
            name="bank_prepared_transfers_state_valid",
        ),
        UniqueConstraint(
            "request_id",
            name="bank_prepared_transfers_request_unique",
        ),
    )

    transaction_id: Mapped[str] = mapped_column(String, primary_key=True)
    request_id: Mapped[str] = mapped_column(String, nullable=False)
    fingerprint: Mapped[str] = mapped_column(Text, nullable=False)
    account: Mapped[str] = mapped_column(
        ForeignKey("accounts.wallet"), nullable=False)
    counterparty: Mapped[str] = mapped_column(String, nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False)
    role: Mapped[str] = mapped_column(String, nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False)
    response_json: Mapped[str | None] = mapped_column(Text)
    client_ip: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc))


class CoordinatorBase(DeclarativeBase):
    pass


class CoordinatedTransfer(CoordinatorBase):
    __tablename__ = "bank_coordinated_transfers"

    transaction_id: Mapped[str] = mapped_column(String, primary_key=True)
    fingerprint: Mapped[str] = mapped_column(Text, nullable=False)
    source_account: Mapped[str] = mapped_column(String, nullable=False)
    destination_account: Mapped[str] = mapped_column(String, nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(20, 2), nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False)
    source_vote: Mapped[bool | None] = mapped_column(Boolean)
    destination_vote: Mapped[bool | None] = mapped_column(Boolean)
    source_ack: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    destination_ack: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    source_result: Mapped[str | None] = mapped_column(Text)
    destination_result: Mapped[str | None] = mapped_column(Text)
    response_json: Mapped[str | None] = mapped_column(Text)
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class Operation:
    action: str
    account: str
    amount: Decimal = Decimal("0")
    destination: str | None = None

    def fingerprint(self) -> str:
        return json.dumps(
            {
                "action": self.action,
                "account": self.account,
                "amount": format(self.amount, "f"),
                "destination": self.destination,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


class BankStore:
    def __init__(self, database_url: str) -> None:
        if not database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError("DATABASE_URL must use the PostgreSQL protocol.")
        self.engine = create_engine(
            database_url,
            pool_pre_ping=True,
            connect_args={
                "options": _postgres_connection_options(
                    database_url,
                    "-c synchronous_commit=on",
                )
            },
        )
        self.session_factory = sessionmaker(
            bind=self.engine,
            class_=Session,
            expire_on_commit=False,
        )

    def initialize(
        self,
        account_range: tuple[str, str] | None = None,
    ) -> None:
        Base.metadata.create_all(self.engine)
        now = datetime.now(timezone.utc)
        accounts = [
            {
                "wallet": "A0001",
                "name": "Sample Customer 0",
                "balance": Decimal("750.00"),
                "created_at": now,
                "updated_at": now,
            },
            {
                "wallet": "A0002",
                "name": "Sample Customer 0B",
                "balance": Decimal("600.00"),
                "created_at": now,
                "updated_at": now,
            },
            {
                "wallet": "A1001",
                "name": "Sample Customer 1",
                "balance": Decimal("1000.00"),
                "created_at": now,
                "updated_at": now,
            },
            {
                "wallet": "A1002",
                "name": "Sample Customer 2",
                "balance": Decimal("500.00"),
                "created_at": now,
                "updated_at": now,
            },
            {
                "wallet": "A1003",
                "name": "Sample Customer 3",
                "balance": Decimal("2000.00"),
                "created_at": now,
                "updated_at": now,
            },
        ]
        if account_range is not None:
            start, end = account_range
            accounts = [
                account for account in accounts
                if start <= account["wallet"] <= end
            ]
        with self.session_factory.begin() as session:
            session.execute(
                insert(Account)
                .values(accounts)
                .on_conflict_do_nothing(index_elements=[Account.wallet])
            )

    def get_balance(self, account: str) -> dict[str, Any]:
        with self.session_factory() as session:
            record = session.scalar(
                select(Account).where(Account.wallet == account)
            )
            if record is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={
                        "error": "unknown_account",
                        "message": f"Account {account} does not exist.",
                    },
                )
            return {
                "status": "success",
                "account": account,
                "balance": float(record.balance),
            }

    def export_snapshot(self) -> dict[str, list[dict[str, Any]]]:
        with self.session_factory() as session:
            accounts = session.scalars(
                select(Account).order_by(Account.wallet)).all()
            transactions = session.scalars(
                select(BankTransaction).order_by(BankTransaction.id)
            ).all()
            operations = session.scalars(
                select(BankOperation).order_by(BankOperation.request_id)
            ).all()
            prepared_transfers = session.scalars(
                select(PreparedTransfer).order_by(
                    PreparedTransfer.transaction_id)
            ).all()
            return {
                "accounts": [
                    {
                        "id": account.id,
                        "wallet": account.wallet,
                        "name": account.name,
                        "balance": str(account.balance),
                        "created_at": account.created_at.isoformat(),
                        "updated_at": account.updated_at.isoformat(),
                    }
                    for account in accounts
                ],
                "transactions": [
                    {
                        "request_id": transaction.request_id,
                        "action": transaction.action,
                        "account": transaction.account,
                        "entry": transaction.entry,
                        "amount": str(transaction.amount),
                        "balance_before": str(transaction.balance_before),
                        "balance_after": str(transaction.balance_after),
                        "client_ip": transaction.client_ip,
                        "created_at": transaction.created_at.isoformat(),
                    }
                    for transaction in transactions
                ],
                "operations": [
                    {
                        "request_id": operation.request_id,
                        "fingerprint": operation.fingerprint,
                        "response_json": operation.response_json,
                        "created_at": operation.created_at.isoformat(),
                    }
                    for operation in operations
                ],
                "prepared_transfers": [
                    {
                        "transaction_id": transfer.transaction_id,
                        "request_id": transfer.request_id,
                        "fingerprint": transfer.fingerprint,
                        "account": transfer.account,
                        "counterparty": transfer.counterparty,
                        "amount": str(transfer.amount),
                        "role": transfer.role,
                        "state": transfer.state,
                        "response_json": transfer.response_json,
                        "client_ip": transfer.client_ip,
                        "created_at": transfer.created_at.isoformat(),
                        "updated_at": transfer.updated_at.isoformat(),
                    }
                    for transfer in prepared_transfers
                ],
            }

    def replica_epoch(self, cluster_id: str) -> int | None:
        with self.session_factory() as session:
            state = session.get(BankReplicaState, cluster_id)
            return state.epoch if state is not None else None

    def set_replica_epoch(self, cluster_id: str, epoch: int) -> None:
        with self.session_factory.begin() as session:
            session.execute(
                insert(BankReplicaState)
                .values(
                    cluster_id=cluster_id,
                    synchronized_at=datetime.now(timezone.utc),
                    epoch=epoch,
                )
                .on_conflict_do_update(
                    index_elements=[BankReplicaState.cluster_id],
                    set_={
                        "synchronized_at": datetime.now(timezone.utc),
                        "epoch": epoch,
                    },
                )
            )

    def restore_snapshot(
        self,
        snapshot: dict[str, Any],
        cluster_id: str,
        epoch: int,
    ) -> None:
        accounts = snapshot.get("accounts")
        transactions = snapshot.get("transactions")
        operations = snapshot.get("operations")
        prepared_transfers = snapshot.get("prepared_transfers", [])
        if not all(isinstance(rows, list) for rows in (accounts, transactions, operations)):
            raise ValueError(
                "Snapshot must contain account, transaction, and operation lists.")
        if not isinstance(prepared_transfers, list):
            raise ValueError("Snapshot prepared_transfers must be a list.")
        if not all(
            isinstance(row, dict)
            for rows in (accounts, transactions, operations)
            for row in rows
        ):
            raise ValueError("Snapshot rows must be objects.")

        def timestamp(value: str) -> datetime:
            return datetime.fromisoformat(value)

        with self.session_factory.begin() as session:
            session.execute(delete(BankTransaction))
            session.execute(delete(BankOperation))
            session.execute(delete(PreparedTransfer))
            session.execute(delete(Account))
            session.add_all(
                Account(
                    id=row["id"],
                    wallet=row["wallet"],
                    name=row["name"],
                    balance=Decimal(row["balance"]),
                    created_at=timestamp(row["created_at"]),
                    updated_at=timestamp(row["updated_at"]),
                )
                for row in accounts
            )
            session.add_all(
                BankTransaction(
                    request_id=row["request_id"],
                    action=row["action"],
                    account=row["account"],
                    entry=row["entry"],
                    amount=Decimal(row["amount"]),
                    balance_before=Decimal(row["balance_before"]),
                    balance_after=Decimal(row["balance_after"]),
                    client_ip=row["client_ip"],
                    created_at=timestamp(row["created_at"]),
                )
                for row in transactions
            )
            session.add_all(
                BankOperation(
                    request_id=row["request_id"],
                    fingerprint=row["fingerprint"],
                    response_json=row["response_json"],
                    created_at=timestamp(row["created_at"]),
                )
                for row in operations
            )
            session.add_all(
                PreparedTransfer(
                    transaction_id=row["transaction_id"],
                    request_id=row["request_id"],
                    fingerprint=row["fingerprint"],
                    account=row["account"],
                    counterparty=row["counterparty"],
                    amount=Decimal(row["amount"]),
                    role=row["role"],
                    state=row["state"],
                    response_json=row.get("response_json"),
                    client_ip=row["client_ip"],
                    created_at=timestamp(row["created_at"]),
                    updated_at=timestamp(row["updated_at"]),
                )
                for row in prepared_transfers
            )
            session.flush()
            session.execute(
                text(
                    "SELECT setval(pg_get_serial_sequence('accounts', 'id'), "
                    "COALESCE(MAX(id), 1), MAX(id) IS NOT NULL) FROM accounts"
                )
            )
            session.execute(
                text(
                    "SELECT setval(pg_get_serial_sequence('bank_transactions', 'id'), "
                    "COALESCE(MAX(id), 1), MAX(id) IS NOT NULL) FROM bank_transactions"
                )
            )
            session.merge(
                BankReplicaState(
                    cluster_id=cluster_id,
                    synchronized_at=datetime.now(timezone.utc),
                    epoch=epoch,
                )
            )

    @staticmethod
    def _lock_2pc_id(session: Session, transaction_id: str) -> None:
        session.scalar(
            select(
                func.pg_advisory_xact_lock(
                    func.hashtextextended(
                        func.concat(
                            func.current_schema(),
                            ":",
                            transaction_id,
                        ),
                        0,
                    )
                )
            )
        )

    @staticmethod
    def _prepared_reservation(
        session: Session,
        account: str,
        role: str,
    ) -> Decimal:
        reservation = session.scalar(
            select(func.coalesce(func.sum(PreparedTransfer.amount), 0)).where(
                PreparedTransfer.account == account,
                PreparedTransfer.role == role,
                PreparedTransfer.state == "prepared",
            )
        )
        return Decimal(reservation)

    @staticmethod
    def _prepared_fingerprint(
        request_id: str,
        source_account: str,
        destination_account: str,
        amount: Decimal,
        role: str,
    ) -> str:
        return json.dumps(
            {
                "request_id": request_id,
                "source_account": source_account,
                "destination_account": destination_account,
                "amount": format(amount, "f"),
                "role": role,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def prepare_cross_shard_transfer(
        self,
        transaction_id: str,
        source_account: str,
        destination_account: str,
        amount: Decimal,
        role: str,
        client_ip: str,
    ) -> dict[str, Any]:
        if (
            role not in {"debit", "credit"}
            or amount <= 0
            or amount > MAX_BALANCE
            or amount.as_tuple().exponent < -2
            or shard_for_account(source_account) == shard_for_account(destination_account)
        ):
            return {"vote": "NO", "reason": "invalid_transfer"}

        account = source_account if role == "debit" else destination_account
        counterparty = (
            destination_account if role == "debit" else source_account
        )
        fingerprint = self._prepared_fingerprint(
            transaction_id,
            source_account,
            destination_account,
            amount,
            role,
        )
        with self.session_factory.begin() as session:
            self._lock_2pc_id(session, transaction_id)
            existing = session.get(PreparedTransfer, transaction_id)
            if existing is not None:
                if existing.fingerprint != fingerprint:
                    return {"vote": "NO", "reason": "transaction_id_conflict"}
                if existing.state == "prepared":
                    return {"vote": "YES"}
                if existing.state == "committed":
                    return {"vote": "YES", "state": "committed"}
                return {"vote": "NO", "reason": "already_aborted"}

            if session.get(BankOperation, transaction_id) is not None:
                return {"vote": "NO", "reason": "request_id_conflict"}

            account_row = session.scalar(
                select(Account)
                .where(Account.wallet == account)
                .with_for_update()
            )
            if account_row is None:
                return {"vote": "NO", "reason": "unknown_account"}

            reserved = self._prepared_reservation(
                session,
                account,
                "debit" if role == "debit" else "credit",
            )
            if role == "debit" and account_row.balance - reserved < amount:
                return {"vote": "NO", "reason": "insufficient_funds"}
            if role == "credit" and account_row.balance + reserved + amount > MAX_BALANCE:
                return {"vote": "NO", "reason": "balance_limit"}

            session.add(
                PreparedTransfer(
                    transaction_id=transaction_id,
                    request_id=transaction_id,
                    fingerprint=fingerprint,
                    account=account,
                    counterparty=counterparty,
                    amount=amount,
                    role=role,
                    state="prepared",
                    client_ip=client_ip,
                )
            )
        return {"vote": "YES"}

    def commit_cross_shard_transfer(
        self,
        transaction_id: str,
    ) -> dict[str, Any]:
        with self.session_factory.begin() as session:
            self._lock_2pc_id(session, transaction_id)
            prepared = session.scalar(
                select(PreparedTransfer)
                .where(PreparedTransfer.transaction_id == transaction_id)
                .with_for_update()
            )
            if prepared is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={
                        "error": "unknown_transaction",
                        "message": "No prepared participant transaction exists.",
                    },
                )
            if prepared.state == "committed":
                if prepared.response_json is None:
                    raise RuntimeError(
                        "Committed participant transaction has no saved response."
                    )
                return json.loads(prepared.response_json)
            if prepared.state == "aborted":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "transaction_aborted",
                        "message": "An aborted participant cannot be committed.",
                    },
                )

            account = session.scalar(
                select(Account)
                .where(Account.wallet == prepared.account)
                .with_for_update()
            )
            if account is None:
                raise RuntimeError(
                    "Prepared participant account disappeared before commit."
                )
            balance_before = account.balance
            if prepared.role == "debit":
                if balance_before < prepared.amount:
                    raise RuntimeError(
                        "Prepared debit is no longer covered by the account balance."
                    )
                balance_after = balance_before - prepared.amount
                entry = "out"
                source_account = prepared.account
                destination_account = prepared.counterparty
            else:
                if balance_before + prepared.amount > MAX_BALANCE:
                    raise RuntimeError(
                        "Prepared credit exceeds the account balance limit."
                    )
                balance_after = balance_before + prepared.amount
                entry = "in"
                source_account = prepared.counterparty
                destination_account = prepared.account

            self._set_balance(account, balance_after)
            session.add(
                BankTransaction(
                    request_id=prepared.request_id,
                    action="transfer",
                    account=prepared.account,
                    entry=entry,
                    amount=prepared.amount,
                    balance_before=balance_before,
                    balance_after=balance_after,
                    client_ip=prepared.client_ip,
                    created_at=datetime.now(timezone.utc),
                )
            )
            operation = Operation(
                "transfer",
                source_account,
                prepared.amount,
                destination_account,
            )
            response = {
                "status": "success",
                "transaction_id": transaction_id,
                "account": prepared.account,
                "role": prepared.role,
                "balance": float(balance_after),
            }
            session.add(
                BankOperation(
                    request_id=prepared.request_id,
                    fingerprint=operation.fingerprint(),
                    response_json=json.dumps(response, separators=(",", ":")),
                )
            )
            prepared.state = "committed"
            prepared.response_json = json.dumps(
                response,
                separators=(",", ":"),
            )
            prepared.updated_at = datetime.now(timezone.utc)
            return response

    def abort_cross_shard_transfer(
        self,
        transaction_id: str,
        source_account: str,
        destination_account: str,
        amount: Decimal,
        role: str,
        client_ip: str,
    ) -> dict[str, str]:
        if role not in {"debit", "credit"}:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "invalid_participant_role",
                    "message": "Participant role must be debit or credit.",
                },
            )
        account = source_account if role == "debit" else destination_account
        counterparty = (
            destination_account if role == "debit" else source_account
        )
        fingerprint = self._prepared_fingerprint(
            transaction_id,
            source_account,
            destination_account,
            amount,
            role,
        )
        with self.session_factory.begin() as session:
            self._lock_2pc_id(session, transaction_id)
            prepared = session.scalar(
                select(PreparedTransfer)
                .where(PreparedTransfer.transaction_id == transaction_id)
                .with_for_update()
            )
            if prepared is None:
                session.add(
                    PreparedTransfer(
                        transaction_id=transaction_id,
                        request_id=transaction_id,
                        fingerprint=fingerprint,
                        account=account,
                        counterparty=counterparty,
                        amount=amount,
                        role=role,
                        state="aborted",
                        client_ip=client_ip,
                    )
                )
            else:
                if prepared.fingerprint != fingerprint:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "transaction_id_conflict",
                            "message": "Transaction ID was reused for another operation.",
                        },
                    )
                if prepared.state == "committed":
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "already_committed",
                            "message": "A committed participant cannot be aborted.",
                        },
                    )
                prepared.state = "aborted"
                prepared.updated_at = datetime.now(timezone.utc)
        return {"state": "aborted"}

    def cross_shard_transfer_status(
        self,
        transaction_id: str,
    ) -> dict[str, Any]:
        with self.session_factory() as session:
            prepared = session.get(PreparedTransfer, transaction_id)
            if prepared is None:
                return {"state": "unknown"}
            return {
                "state": prepared.state,
                "role": prepared.role,
                "account": prepared.account,
                "response": (
                    json.loads(prepared.response_json)
                    if prepared.response_json
                    else None
                ),
            }

    def execute(
        self,
        operation: Operation,
        request_id: str,
        client_ip: str,
        cluster_id: str | None = None,
        epoch: int | None = None,
    ) -> dict[str, Any]:
        with self.session_factory.begin() as session:
            if operation.amount <= 0 or operation.amount > MAX_BALANCE:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "invalid_amount",
                        "message": "amount is outside the supported range.",
                    },
                )

            session.scalar(
                select(
                    func.pg_advisory_xact_lock(
                        func.hashtextextended(
                            func.concat(func.current_schema(),
                                        ":", request_id),
                            0,
                        )
                    )
                )
            )
            if cluster_id is not None and epoch is not None:
                session.execute(
                    insert(BankReplicaState)
                    .values(
                        cluster_id=cluster_id,
                        synchronized_at=datetime.now(timezone.utc),
                        epoch=epoch,
                    )
                    .on_conflict_do_update(
                        index_elements=[BankReplicaState.cluster_id],
                        set_={
                            "synchronized_at": datetime.now(timezone.utc),
                            "epoch": epoch,
                        },
                    )
                )
            existing = session.get(BankOperation, request_id)
            fingerprint = operation.fingerprint()
            if existing is not None:
                if existing.fingerprint != fingerprint:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "request_id_conflict",
                            "message": "This request ID was already used for a different operation.",
                        },
                    )
                return json.loads(existing.response_json)
            prepared_request = session.scalar(
                select(PreparedTransfer).where(
                    PreparedTransfer.request_id == request_id
                )
            )
            if prepared_request is not None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "request_id_conflict",
                        "message": "This request ID belongs to a coordinated transfer.",
                    },
                )

            destination: Account | None = None
            if operation.action == "transfer":
                if operation.destination is None:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "missing_field",
                            "message": "destination is required for a transfer.",
                        },
                    )
                if operation.destination == operation.account:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "invalid_transfer",
                            "message": "Source and destination accounts must differ.",
                        },
                    )
                locked_accounts = session.scalars(
                    select(Account)
                    .where(
                        Account.wallet.in_(
                            [operation.account, operation.destination]
                        )
                    )
                    .order_by(Account.wallet)
                    .with_for_update()
                ).all()
                accounts = {
                    account.wallet: account for account in locked_accounts}
                source = accounts.get(operation.account)
                destination = accounts.get(operation.destination)
                if source is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail={
                            "error": "unknown_account",
                            "message": f"Account {operation.account} does not exist.",
                        },
                    )
                if destination is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail={
                            "error": "unknown_account",
                            "message": f"Account {operation.destination} does not exist.",
                        },
                    )
            else:
                source = session.scalar(
                    select(Account)
                    .where(Account.wallet == operation.account)
                    .with_for_update()
                )
                if source is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail={
                            "error": "unknown_account",
                            "message": f"Account {operation.account} does not exist.",
                        },
                    )

            if operation.action == "deposit":
                previous_balance = source.balance
                new_balance = previous_balance + operation.amount
                reserved_incoming = self._prepared_reservation(
                    session,
                    source.wallet,
                    "credit",
                )
                if new_balance + reserved_incoming > MAX_BALANCE:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "invalid_amount",
                            "message": "resulting balance exceeds the supported range.",
                        },
                    )
                self._set_balance(source, new_balance)
                self._add_transaction(
                    session, operation, source, "in", previous_balance,
                    new_balance, request_id, client_ip,
                )
                response: dict[str, Any] = {
                    "status": "success",
                    "account": operation.account,
                    "balance": float(new_balance),
                }
            elif operation.action == "withdraw":
                reserved_outgoing = self._prepared_reservation(
                    session,
                    source.wallet,
                    "debit",
                )
                if source.balance - reserved_outgoing < operation.amount:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "insufficient_funds",
                            "message": "Insufficient funds.",
                        },
                    )
                previous_balance = source.balance
                new_balance = previous_balance - operation.amount
                self._set_balance(source, new_balance)
                self._add_transaction(
                    session, operation, source, "out", previous_balance,
                    new_balance, request_id, client_ip,
                )
                response = {
                    "status": "success",
                    "account": operation.account,
                    "balance": float(new_balance),
                }
            elif operation.action == "transfer":
                if destination is None:
                    raise RuntimeError("Transfer destination was not locked.")
                if source.balance < operation.amount:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "insufficient_funds",
                            "message": "Insufficient funds.",
                        },
                    )
                source_before = source.balance
                destination_before = destination.balance
                source_after = source_before - operation.amount
                destination_after = destination_before + operation.amount
                reserved_outgoing = self._prepared_reservation(
                    session,
                    source.wallet,
                    "debit",
                )
                reserved_incoming = self._prepared_reservation(
                    session,
                    destination.wallet,
                    "credit",
                )
                if source_before - reserved_outgoing < operation.amount:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "insufficient_funds",
                            "message": "Funds are reserved by a pending transfer.",
                        },
                    )
                if destination_after + reserved_incoming > MAX_BALANCE:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "invalid_amount",
                            "message": "Resulting destination balance exceeds the supported range.",
                        },
                    )
                self._set_balance(source, source_after)
                self._set_balance(destination, destination_after)
                self._add_transaction(
                    session,
                    operation,
                    source,
                    "out",
                    source_before,
                    source_after,
                    request_id,
                    client_ip,
                )
                self._add_transaction(
                    session,
                    operation,
                    destination,
                    "in",
                    destination_before,
                    destination_after,
                    request_id,
                    client_ip,
                )
                response = {
                    "status": "success",
                    "source_account": operation.account,
                    "source_balance": float(source_after),
                    "destination": operation.destination,
                    "destination_balance": float(destination_after),
                }
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "unsupported_action",
                        "message": f"Action {operation.action!r} is not supported.",
                    },
                )

            session.add(
                BankOperation(
                    request_id=request_id,
                    fingerprint=fingerprint,
                    response_json=json.dumps(response, separators=(",", ":")),
                )
            )
            return response

    @staticmethod
    def _set_balance(account: Account, amount: Decimal) -> None:
        account.balance = amount
        account.updated_at = datetime.now(timezone.utc)

    @staticmethod
    def _add_transaction(
        session: Session,
        operation: Operation,
        account: Account,
        entry: str,
        balance_before: Decimal,
        balance_after: Decimal,
        request_id: str,
        client_ip: str,
    ) -> None:
        session.add(
            BankTransaction(
                request_id=request_id,
                action=operation.action,
                account=account.wallet,
                entry=entry,
                amount=operation.amount,
                balance_before=balance_before,
                balance_after=balance_after,
                client_ip=client_ip,
                created_at=datetime.now(timezone.utc),
            )
        )


class ShardedBankStore:
    def __init__(
        self,
        shard_a_database_url: str,
        shard_b_database_url: str,
    ) -> None:
        self.stores = {
            "shard-a": BankStore(shard_a_database_url),
            "shard-b": BankStore(shard_b_database_url),
        }

    def initialize(self) -> None:
        for shard_id, store in self.stores.items():
            store.initialize(account_range_for_shard(shard_id))

    def get_balance(self, account: str) -> dict[str, Any]:
        return self.stores[shard_for_account(account)].get_balance(account)

    def execute(
        self,
        operation: Operation,
        request_id: str,
        client_ip: str,
        cluster_id: str | None = None,
        epoch: int | None = None,
    ) -> dict[str, Any]:
        shard_id = shard_for_account(operation.account)
        if operation.action == "transfer" and operation.destination is not None:
            destination_shard = shard_for_account(operation.destination)
            if destination_shard != shard_id:
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
        return self.stores[shard_id].execute(
            operation,
            request_id,
            client_ip,
            cluster_id,
            epoch,
        )

    def export_snapshot(self) -> dict[str, list[dict[str, Any]]]:
        snapshots = [store.export_snapshot() for store in self.stores.values()]
        return {
            key: [row for snapshot in snapshots for row in snapshot[key]]
            for key in (
                "accounts",
                "transactions",
                "operations",
                "prepared_transfers",
            )
        }

    def restore_snapshot(
        self,
        snapshot: dict[str, Any],
        cluster_id: str,
        epoch: int,
    ) -> None:
        accounts = snapshot.get("accounts")
        transactions = snapshot.get("transactions")
        operations = snapshot.get("operations")
        prepared_transfers = snapshot.get("prepared_transfers", [])
        if not all(
            isinstance(rows, list)
            for rows in (accounts, transactions, operations)
        ) or not isinstance(prepared_transfers, list):
            raise ValueError(
                "Snapshot must contain account, transaction, operation, and prepared transfer lists."
            )

        snapshots_by_shard: dict[str, dict[str, list[dict[str, Any]]]] = {
            shard_id: {
                "accounts": [],
                "transactions": [],
                "operations": [],
                "prepared_transfers": [],
            }
            for shard_id in self.stores
        }
        for account in accounts:
            if not isinstance(account, dict) or not isinstance(
                account.get("wallet"), str
            ):
                raise ValueError("Snapshot account rows must have a wallet.")
            shard_id = shard_for_account(account["wallet"])
            snapshots_by_shard[shard_id]["accounts"].append(account)

        operation_shards: dict[str, str] = {}
        for transaction in transactions:
            if not isinstance(transaction, dict):
                raise ValueError("Snapshot transaction rows must be objects.")
            request_id = transaction.get("request_id")
            account = transaction.get("account")
            if not isinstance(request_id, str) or not isinstance(account, str):
                raise ValueError(
                    "Snapshot transaction rows must include request_id and account."
                )
            shard_id = shard_for_account(account)
            snapshots_by_shard[shard_id]["transactions"].append(transaction)
            operation_shards[request_id] = shard_id

        for operation in operations:
            if not isinstance(operation, dict):
                raise ValueError("Snapshot operation rows must be objects.")
            request_id = operation.get("request_id")
            if not isinstance(request_id, str) or request_id not in operation_shards:
                raise ValueError(
                    "Snapshot operations must have a matching transaction."
                )
            shard_id = operation_shards[request_id]
            snapshots_by_shard[shard_id]["operations"].append(operation)

        for transfer in prepared_transfers:
            if not isinstance(transfer, dict) or not isinstance(
                transfer.get("account"), str
            ):
                raise ValueError(
                    "Snapshot prepared transfer rows must include an account."
                )
            shard_id = shard_for_account(transfer["account"])
            snapshots_by_shard[shard_id]["prepared_transfers"].append(transfer)

        for shard_id, store in self.stores.items():
            store.restore_snapshot(
                snapshots_by_shard[shard_id],
                cluster_id,
                epoch,
            )

    def replica_epoch(self, cluster_id: str) -> int | None:
        epochs = [
            store.replica_epoch(cluster_id)
            for store in self.stores.values()
        ]
        if any(epoch is None for epoch in epochs):
            return None
        if len(set(epochs)) != 1:
            return None
        return epochs[0]

    def set_replica_epoch(self, cluster_id: str, epoch: int) -> None:
        for store in self.stores.values():
            store.set_replica_epoch(cluster_id, epoch)


class CoordinatorStore:
    def __init__(self, database_url: str) -> None:
        if not database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError(
                "BANK_COORDINATOR_DATABASE_URL must use the PostgreSQL protocol."
            )
        self.engine = create_engine(
            database_url,
            pool_pre_ping=True,
            connect_args={
                "options": _postgres_connection_options(
                    database_url,
                    "-c synchronous_commit=on",
                )
            },
        )
        self.session_factory = sessionmaker(
            bind=self.engine,
            class_=Session,
            expire_on_commit=False,
        )

    def initialize(self) -> None:
        CoordinatorBase.metadata.create_all(self.engine)

    @staticmethod
    def _as_dict(transaction: CoordinatedTransfer) -> dict[str, Any]:
        return {
            "transaction_id": transaction.transaction_id,
            "fingerprint": transaction.fingerprint,
            "source_account": transaction.source_account,
            "destination_account": transaction.destination_account,
            "amount": str(transaction.amount),
            "state": transaction.state,
            "source_vote": transaction.source_vote,
            "destination_vote": transaction.destination_vote,
            "source_ack": transaction.source_ack,
            "destination_ack": transaction.destination_ack,
            "source_result": (
                json.loads(transaction.source_result)
                if transaction.source_result
                else None
            ),
            "destination_result": (
                json.loads(transaction.destination_result)
                if transaction.destination_result
                else None
            ),
            "response": (
                json.loads(transaction.response_json)
                if transaction.response_json
                else None
            ),
            "error_message": transaction.error_message,
        }

    def begin(
        self,
        transaction_id: str,
        fingerprint: str,
        source_account: str,
        destination_account: str,
        amount: Decimal,
    ) -> dict[str, Any]:
        with self.session_factory.begin() as session:
            session.execute(
                insert(CoordinatedTransfer)
                .values(
                    transaction_id=transaction_id,
                    fingerprint=fingerprint,
                    source_account=source_account,
                    destination_account=destination_account,
                    amount=amount,
                    state="PREPARING",
                    source_vote=None,
                    destination_vote=None,
                    source_ack=False,
                    destination_ack=False,
                )
                .on_conflict_do_nothing(
                    index_elements=[CoordinatedTransfer.transaction_id]
                )
            )
            transaction = session.get(CoordinatedTransfer, transaction_id)
            if transaction is None:
                raise RuntimeError("Coordinator transaction was not persisted.")
            if transaction.fingerprint != fingerprint:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "request_id_conflict",
                        "message": "This request ID was already used for a different operation.",
                    },
                )
            return self._as_dict(transaction)

    def get(self, transaction_id: str) -> dict[str, Any] | None:
        with self.session_factory() as session:
            transaction = session.get(CoordinatedTransfer, transaction_id)
            return self._as_dict(transaction) if transaction else None

    def record_vote(
        self,
        transaction_id: str,
        participant: str,
        vote: bool,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        if participant not in {"source", "destination"}:
            raise ValueError("Participant must be source or destination.")
        field = f"{participant}_vote"
        with self.session_factory.begin() as session:
            transaction = session.scalar(
                select(CoordinatedTransfer)
                .where(CoordinatedTransfer.transaction_id == transaction_id)
                .with_for_update()
            )
            if transaction is None:
                raise KeyError(transaction_id)
            previous_vote = getattr(transaction, field)
            if previous_vote is not None and previous_vote != vote:
                raise RuntimeError("Participant vote cannot be changed.")
            if transaction.state != "PREPARING":
                return self._as_dict(transaction)
            setattr(transaction, field, vote)
            if error_message and transaction.error_message is None:
                transaction.error_message = error_message
            transaction.updated_at = datetime.now(timezone.utc)
            return self._as_dict(transaction)

    def decide(self, transaction_id: str, decision: str) -> dict[str, Any]:
        if decision not in {"COMMIT", "ABORT"}:
            raise ValueError("Coordinator decision must be COMMIT or ABORT.")
        with self.session_factory.begin() as session:
            transaction = session.scalar(
                select(CoordinatedTransfer)
                .where(CoordinatedTransfer.transaction_id == transaction_id)
                .with_for_update()
            )
            if transaction is None:
                raise KeyError(transaction_id)
            if transaction.state in {"COMMIT", "COMMITTED", "ABORT", "ABORTED"}:
                if transaction.state not in {
                    decision,
                    "COMMITTED" if decision == "COMMIT" else "ABORTED",
                }:
                    raise RuntimeError("A durable coordinator decision cannot change.")
                return self._as_dict(transaction)
            if transaction.state != "PREPARING":
                raise RuntimeError("Invalid coordinator transaction state.")
            if decision == "COMMIT" and not (
                transaction.source_vote is True
                and transaction.destination_vote is True
            ):
                raise RuntimeError("COMMIT requires YES from both participants.")
            transaction.state = decision
            transaction.updated_at = datetime.now(timezone.utc)
            return self._as_dict(transaction)

    def record_ack(
        self,
        transaction_id: str,
        participant: str,
        result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if participant not in {"source", "destination"}:
            raise ValueError("Participant must be source or destination.")
        with self.session_factory.begin() as session:
            transaction = session.scalar(
                select(CoordinatedTransfer)
                .where(CoordinatedTransfer.transaction_id == transaction_id)
                .with_for_update()
            )
            if transaction is None:
                raise KeyError(transaction_id)
            if transaction.state not in {"COMMIT", "ABORT"}:
                if transaction.state in {"COMMITTED", "ABORTED"}:
                    return self._as_dict(transaction)
                raise RuntimeError(
                    "Participant acknowledgements require a durable decision."
                )
            setattr(transaction, f"{participant}_ack", True)
            if result is not None:
                setattr(
                    transaction,
                    f"{participant}_result",
                    json.dumps(result, separators=(",", ":")),
                )
            if transaction.source_ack and transaction.destination_ack:
                if transaction.state == "COMMIT":
                    source_result = json.loads(transaction.source_result or "{}")
                    destination_result = json.loads(
                        transaction.destination_result or "{}"
                    )
                    response = {
                        "status": "success",
                        "transaction_id": transaction_id,
                        "source_account": transaction.source_account,
                        "source_balance": source_result["balance"],
                        "destination": transaction.destination_account,
                        "destination_balance": destination_result["balance"],
                    }
                    transaction.response_json = json.dumps(
                        response,
                        separators=(",", ":"),
                    )
                    transaction.state = "COMMITTED"
                else:
                    transaction.state = "ABORTED"
            transaction.updated_at = datetime.now(timezone.utc)
            return self._as_dict(transaction)

    def pending(self) -> list[dict[str, Any]]:
        with self.session_factory() as session:
            transactions = session.scalars(
                select(CoordinatedTransfer)
                .where(
                    CoordinatedTransfer.state.in_(
                        ("PREPARING", "COMMIT", "ABORT")
                    )
                )
                .order_by(CoordinatedTransfer.created_at)
            ).all()
            return [self._as_dict(transaction) for transaction in transactions]


class BankWitness:
    def __init__(self, database_url: str, cluster_id: str, logger: Any) -> None:
        if not database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError(
                "BANK_WITNESS_DATABASE_URL must use the PostgreSQL protocol.")
        self.cluster_id = cluster_id
        self.logger = logger
        self.engine: Engine = create_engine(
            database_url,
            pool_pre_ping=True,
            connect_args={
                "connect_timeout": 3,
                "options": _postgres_connection_options(
                    database_url,
                    "-c synchronous_commit=on",
                    "-c statement_timeout=3000",
                ),
            },
        )
        self._connection: Connection | None = None
        self._backend_pid: int | None = None
        self._epoch: int | None = None
        self._lock = Lock()

    def initialize(self) -> None:
        WitnessBase.metadata.create_all(self.engine)

    def _release_candidate(self, connection: Connection) -> None:
        try:
            connection.rollback()
            released = connection.scalar(
                select(
                    func.pg_advisory_unlock(
                        func.hashtextextended(self.cluster_id, 0)
                    )
                )
            )
            connection.commit()
            if not released:
                self.logger.error(
                    "PostgreSQL witness candidate lock was not held.")
        except SQLAlchemyError:
            self.logger.warning(
                "Could not release PostgreSQL witness candidate lock; closing its connection.",
                exc_info=True,
            )
            connection.invalidate()
        finally:
            connection.close()

    def try_acquire(
        self,
        node_id: str,
        local_epoch: int | None,
        allow_initial: bool = False,
    ) -> int | None:
        with self._lock:
            if self._connection is not None:
                if self._owns_lock():
                    return self._epoch
                self._close_connection()

            connection: Connection | None = None
            lock_acquired = False
            try:
                connection = self.engine.connect()
                acquired, backend_pid = connection.execute(
                    select(
                        func.pg_try_advisory_lock(
                            func.hashtextextended(self.cluster_id, 0)
                        ),
                        func.pg_backend_pid(),
                    )
                ).one()
                if not acquired:
                    connection.rollback()
                    connection.close()
                    return None
                lock_acquired = True
                previous = connection.execute(
                    select(WitnessLeader.node_id, WitnessLeader.epoch).where(
                        WitnessLeader.cluster_id == self.cluster_id
                    )
                ).first()
                if previous is None:
                    if not allow_initial:
                        self._release_candidate(connection)
                        return None
                    epoch = 1
                    connection.execute(
                        WitnessLeader.__table__.insert().values(
                            cluster_id=self.cluster_id,
                            node_id=node_id,
                            epoch=epoch,
                        )
                    )
                else:
                    previous_node, previous_epoch = previous
                    local_is_current = (
                        local_epoch is not None and local_epoch >= previous_epoch
                    )
                    recovering_own_promotion = (
                        previous_node == node_id
                        and local_epoch == previous_epoch - 1
                    )
                    if not local_is_current and not recovering_own_promotion:
                        self._release_candidate(connection)
                        return None
                    epoch = previous_epoch + 1
                    connection.execute(
                        update(WitnessLeader)
                        .where(WitnessLeader.cluster_id == self.cluster_id)
                        .values(node_id=node_id, epoch=epoch)
                    )
                connection.commit()
                self._connection = connection
                self._backend_pid = backend_pid
                self._epoch = epoch
                lock_acquired = False
                return epoch
            except SQLAlchemyError:
                if connection is not None:
                    if lock_acquired:
                        self._release_candidate(connection)
                    else:
                        connection.close()
                raise

    def is_current_leader(self, node_id: str, epoch: int) -> bool:
        leader = self.current_leader()
        return leader == (node_id, epoch)

    def current_leader(self) -> tuple[str, int] | None:
        try:
            with self.engine.connect() as connection:
                result = connection.execute(
                    select(WitnessLeader.node_id, WitnessLeader.epoch).where(
                        WitnessLeader.cluster_id == self.cluster_id
                    )
                ).first()
            if result is None:
                return None
            return str(result[0]), int(result[1])
        except SQLAlchemyError:
            self.logger.warning(
                "Could not verify witness leader term.", exc_info=True)
            return None

    def owns_lock(self) -> bool:
        with self._lock:
            return self._owns_lock()

    def _owns_lock(self) -> bool:
        if self._connection is None or self._backend_pid is None:
            return False
        try:
            backend_pid = self._connection.scalar(
                select(func.pg_backend_pid()))
            self._connection.commit()
        except SQLAlchemyError:
            self.logger.warning(
                "Could not verify PostgreSQL witness lock.", exc_info=True)
            self._close_connection()
            return False
        if backend_pid != self._backend_pid:
            self._close_connection()
            return False
        return True

    def _close_connection(self) -> None:
        connection, self._connection = self._connection, None
        self._backend_pid = None
        self._epoch = None
        if connection is not None:
            try:
                connection.invalidate()
            except SQLAlchemyError:
                self.logger.warning(
                    "Could not invalidate the PostgreSQL witness connection.",
                    exc_info=True,
                )
            finally:
                try:
                    connection.close()
                except SQLAlchemyError:
                    self.logger.warning(
                        "Could not close the PostgreSQL witness connection.",
                        exc_info=True,
                    )

    def close(self) -> None:
        with self._lock:
            self._close_connection()
        self.engine.dispose()
