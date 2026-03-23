"""Normalize raw payload (e.g. Tally vouchers) into canonical accounting records."""

from typing import Any


def normalize_accounting_data(payload: Any) -> list[dict[str, Any]]:
    """
    Parse and normalize accounting data from ingest payload.
    Expects payload to be a list of voucher-like dicts (e.g. from Tally daybook)
    with optional LedgerEntries. Returns list of normalized voucher dicts.
    """
    if payload is None:
        return []
    if isinstance(payload, dict):
        # Single voucher or wrapped in a key
        if "vouchers" in payload:
            payload = payload["vouchers"]
        elif "GUID" in payload or "VoucherNumber" in payload or "VoucherType" in payload:
            payload = [payload]
        else:
            return []
    if not isinstance(payload, list):
        return []

    normalized: list[dict[str, Any]] = []
    for raw in payload:
        if not isinstance(raw, dict):
            continue
        v = {
            "remote_id": _str(raw.get("GUID") or raw.get("id")),
            "external_id": _str(raw.get("GUID") or raw.get("id")),
            "voucher_number": _str(raw.get("VoucherNumber") or raw.get("voucher_number")),
            "voucher_type": _str(raw.get("VoucherType") or raw.get("VoucherTypeName") or raw.get("voucher_type")),
            "date": _str(raw.get("Date") or raw.get("date")),
            "narration": _str(raw.get("Narration") or raw.get("narration")),
            "ledger_entries": _normalize_ledger_entries(raw),
        }
        normalized.append(v)
    return normalized


def _str(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _normalize_ledger_entries(raw_voucher: dict) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for key in ("LedgerEntries", "ledger_entries", "ALLLEDGERENTRIES.LIST", "LEDGERENTRIES.LIST"):
        val = raw_voucher.get(key)
        if isinstance(val, dict):
            val = [val]
        if not isinstance(val, list):
            continue
        for item in val:
            if not isinstance(item, dict):
                continue
            name = _str(item.get("LedgerName") or item.get("ledger_name") or item.get("NAME"))
            amount = item.get("Amount") or item.get("amount") or 0
            try:
                amount = float(amount)
            except (TypeError, ValueError):
                amount = 0.0
            deemed = _str(item.get("IsDeemedPositive") or item.get("is_deemed_positive") or "Yes")
            is_debit = deemed.lower() in ("yes", "true", "1")
            key_ = (name, str(amount), is_debit)
            if key_ not in seen:
                seen.add(key_)
                entries.append({
                    "ledger_name": name,
                    "amount": amount,
                    "is_deemed_positive": deemed,
                    "is_debit": is_debit,
                })
    return entries
