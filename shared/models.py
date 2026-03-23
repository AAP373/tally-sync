from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class Ledger:
    external_id: str
    name: str
    group: str = ""
    is_active: bool = True


@dataclass(frozen=True)
class Voucher:
    external_id: str
    voucher_type: str
    date: str
    amount: float
    ledger_name: str = ""
    narration: str = ""


@dataclass(frozen=True)
class SyncEnvelope:
    """
    Minimal wire format for agent -> server sync.

    `sync_id` enables server-side idempotency (dedupe on repeated uploads).
    """

    sync_id: str
    ledgers: List[Ledger] = field(default_factory=list)
    vouchers: List[Voucher] = field(default_factory=list)
    last_ledger_sync_token: Optional[str] = None
    last_voucher_sync_token: Optional[str] = None

    def model_dump(self) -> Dict[str, Any]:
        return asdict(self)

    def model_dump_json(self) -> str:
        return json.dumps(self.model_dump(), ensure_ascii=False, separators=(",", ":"))


__all__ = ["Ledger", "Voucher", "SyncEnvelope"]

