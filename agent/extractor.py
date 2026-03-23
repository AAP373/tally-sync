"""
Domain extractor for transforming raw Tally data into shared models.

Architecture notes
------------------
- This module is the "mapping" layer between the Tally-specific schema and
  the platform's canonical accounting schema (`shared.models`).
- It should not talk to the network or disk; it works purely in memory.
- Agent workflows (see `sync_worker.py`) will:
    - Use `TallyClient` to fetch raw data.
    - Call functions in this module to convert that data into Pydantic models.
    - Hand those models off to persistence/crypto/sync logic.
"""

from __future__ import annotations

from typing import Iterable, List

from shared.models import Ledger, Voucher


def _first_present(raw: dict, keys: Iterable[str], default=None):
    for k in keys:
        if k in raw and raw.get(k) not in (None, ""):
            return raw.get(k)
        lk = k.lower()
        for cand in (lk, lk.upper(), lk.title()):
            if cand in raw and raw.get(cand) not in (None, ""):
                return raw.get(cand)
    return default


def extract_ledgers(raw_ledgers: Iterable[dict]) -> List[Ledger]:
    """
    Convert raw ledger dictionaries from Tally into normalized `Ledger` models.

    At this stage we hide any Tally-specific field names and shape everything
    into a canonical structure shared with the server.
    """
    ledgers: List[Ledger] = []
    for raw in raw_ledgers:
        ledger = Ledger(
            external_id=str(_first_present(raw, ("external_id", "id", "guid", "GUID"), "")),
            name=str(_first_present(raw, ("name", "Name", "LEDGERNAME"), "")),
            group=str(_first_present(raw, ("group", "Group", "PARENT", "Parent"), "")),
            is_active=bool(_first_present(raw, ("is_active", "IsActive", "ACTIVE"), True)),
        )
        ledgers.append(ledger)
    return ledgers


def extract_vouchers(raw_vouchers: Iterable[dict]) -> List[Voucher]:
    """
    Convert raw voucher dictionaries from Tally into normalized `Voucher` models.
    """
    vouchers: List[Voucher] = []
    for raw in raw_vouchers:
        # Tally shapes vary: sometimes Amount is per ledger entry, sometimes only in entries list.
        amount = _first_present(raw, ("amount", "Amount"), None)
        if amount is None:
            entries = raw.get("LedgerEntries") or raw.get("ledger_entries") or []
            if isinstance(entries, list) and entries:
                entry_amt = entries[0].get("Amount") if isinstance(entries[0], dict) else None
                amount = entry_amt if entry_amt is not None else 0.0
            else:
                amount = 0.0
        voucher = Voucher(
            external_id=str(_first_present(raw, ("external_id", "id", "guid", "GUID"), "")),
            voucher_type=str(_first_present(raw, ("voucher_type", "type", "VoucherType", "VOUCHERTYPE"), "")),
            date=str(_first_present(raw, ("date", "Date", "DATE"), "")),
            amount=float(amount or 0.0),
            ledger_name=str(_first_present(raw, ("ledger_name", "LedgerName", "LEDGERNAME"), "")),
            narration=str(_first_present(raw, ("narration", "Narration", "NARRATION"), "")),
        )
        vouchers.append(voucher)
    return vouchers


__all__ = ["extract_ledgers", "extract_vouchers"]

