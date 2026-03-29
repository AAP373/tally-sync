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
        # Extract ledger entries from parsed dictionary (not XML)
        ledger_entries = raw.get("LedgerEntries", [])
        
        # Handle both single entry and list of entries
        if isinstance(ledger_entries, dict):
            ledger_entries = [ledger_entries]
        
        # Map to expected format
        normalized_entries = []
        for entry in ledger_entries:
            if isinstance(entry, dict):
                normalized_entries.append({
                    "ledger_name": entry.get("LedgerName", ""),
                    "amount": entry.get("Amount", 0),
                })
        
        voucher = Voucher(
            external_id=str(_first_present(raw, ("GUID", "external_id"), "")),
            voucher_type=str(_first_present(raw, ("VoucherType", "voucher_type"), "")),
            date=str(_first_present(raw, ("Date", "date"), "")),
            amount=float(sum(entry.get("Amount", 0) for entry in normalized_entries)),
            narration=str(_first_present(raw, ("Narration", "narration"), "")),
            ledger_entries=normalized_entries,
        )
        vouchers.append(voucher)
    return vouchers


__all__ = ["extract_ledgers", "extract_vouchers"]