"""Relational accounting schema: ledgers, vouchers, voucher_entries."""

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Text, DateTime, Numeric, ForeignKey, UniqueConstraint, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, Session


class Base(DeclarativeBase):
    pass


class Ledger(Base):
    __tablename__ = "ledgers"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)


class Voucher(Base):
    __tablename__ = "vouchers"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    remote_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True, index=True)
    voucher_number: Mapped[str] = mapped_column(Text, nullable=False, default="")
    voucher_type: Mapped[str] = mapped_column(Text, nullable=False, default="")
    date: Mapped[str] = mapped_column(Text, nullable=False, default="")
    narration: Mapped[str] = mapped_column(Text, default="")

    entries: Mapped[list["VoucherEntry"]] = relationship(
        "VoucherEntry", back_populates="voucher", cascade="all, delete-orphan"
    )


class VoucherEntry(Base):
    __tablename__ = "voucher_entries"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    voucher_id: Mapped[int] = mapped_column(
        ForeignKey("vouchers.id", ondelete="CASCADE"), nullable=False, index=True
    )
    ledger_name: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False, default=Decimal("0"))
    is_debit: Mapped[bool] = mapped_column(nullable=False, default=True)

    voucher: Mapped["Voucher"] = relationship("Voucher", back_populates="entries")


def get_engine(database_url: str):
    return create_engine(database_url, pool_pre_ping=True)


def init_db(engine) -> None:
    Base.metadata.create_all(engine)


def _ensure_ledger(session: Session, name: str) -> None:
    if not name:
        return
    existing = session.execute(select(Ledger).where(Ledger.name == name)).scalar_one_or_none()
    if existing is None:
        session.add(Ledger(name=name))


def store_normalized(engine, agent_id: str, normalized: list[dict[str, Any]]) -> int:
    """
    Persist normalized vouchers and voucher_entries. Uses remote_id uniqueness
    for idempotency: vouchers with an existing remote_id are skipped.
    Returns number of vouchers actually inserted.
    """
    stored = 0
    with Session(engine) as session:
        for v in normalized:
            remote_id = (v.get("remote_id") or v.get("external_id") or "").strip()
            if not remote_id:
                continue
            existing = session.execute(select(Voucher).where(Voucher.remote_id == remote_id)).scalar_one_or_none()
            if existing is not None:
                continue
            voucher = Voucher(
                remote_id=remote_id,
                voucher_number=v.get("voucher_number") or "",
                voucher_type=v.get("voucher_type") or "",
                date=v.get("date") or "",
                narration=v.get("narration") or "",
            )
            session.add(voucher)
            session.flush()
            for e in v.get("ledger_entries") or []:
                ledger_name = (e.get("ledger_name") or "").strip()
                _ensure_ledger(session, ledger_name)
                amt = e.get("amount")
                if amt is None:
                    amt = Decimal("0")
                elif not isinstance(amt, Decimal):
                    amt = Decimal(str(amt))
                is_debit = e.get("is_debit", None)
                if is_debit is None:
                    deemed = (e.get("is_deemed_positive") or "Yes").strip().lower()
                    is_debit = deemed in ("yes", "true", "1")
                entry = VoucherEntry(
                    voucher_id=voucher.id,
                    ledger_name=ledger_name,
                    amount=amt,
                    is_debit=bool(is_debit),
                )
                session.add(entry)
            stored += 1
        session.commit()
    return stored
