from __future__ import annotations
from dataclasses import dataclass, field

import asyncio
import logging
import time
import uuid
from typing import Optional, List
from datetime import date

import httpx

from agent.crypto import CryptoContext
from agent.extractor import extract_ledgers, extract_vouchers
from agent.persistence import Persistence
from agent.tally_client import TallyClient
from shared.models import SyncEnvelope

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LedgerEntry:
    ledger_name: str
    amount: float
    is_debit: bool = False

@dataclass(frozen=True)
class Voucher:
    external_id: str
    voucher_type: str
    date: str
    amount: float
    ledger_name: str = ""
    narration: str = ""
    ledger_entries: List[LedgerEntry] = field(default_factory=list)


class SyncWorker:

    def __init__(
        self,
        tally_client: TallyClient,
        persistence: Persistence,
        crypto: CryptoContext,
        server_base_url: str,
        interval_seconds: int = 60,
    ):
        self.tally_client = tally_client
        self.persistence = persistence
        self.crypto = crypto
        self.server_base_url = server_base_url.rstrip("/")
        self.interval_seconds = interval_seconds
        self._stopped = asyncio.Event()
        self._send_failures = 0
        self._next_send_time_monotonic: float = 0.0

    async def start(self) -> None:
        await self.persistence.init()
        logger.info("Starting sync loop towards %s", self.server_base_url)
        try:
            while not self._stopped.is_set():
                now = time.monotonic()
                if now < self._next_send_time_monotonic:
                    await asyncio.sleep(self._next_send_time_monotonic - now)
                state = await self.persistence.get_state()
                if state.queue_status in ("RECONCILING", "STALE"):
                    logger.info("Reconciliation mode active (queue_status=%s)", state.queue_status)
                    await self._run_reconciliation_once()
                else:
                    await self._run_once()
                try:
                    await asyncio.wait_for(
                        self._stopped.wait(), timeout=self.interval_seconds
                    )
                except asyncio.TimeoutError:
                    continue
        finally:
            await self.persistence.close()

    def stop(self) -> None:
        self._stopped.set()

    async def _run_once(self) -> None:
        logger.debug("Starting sync iteration.")
        if not self.tally_client.health_check():
            logger.warning("Tally not reachable; will retry later.")
            return

        state = await self.persistence.get_state()

        today = date.today()
        if today.month >= 4:
            start_year = today.year
        else:
            start_year = today.year - 1
        from_date = f"{start_year}0401"
        to_date = f"{start_year + 1}0331"

        raw_ledgers = []
        raw_vouchers = self.tally_client.get_daybook(from_date, to_date)

        ledgers = extract_ledgers(raw_ledgers)
        vouchers = extract_vouchers(raw_vouchers)
        logger.info("[SYNC] fetched %d vouchers (daybook %s..%s)", len(vouchers), from_date, to_date)

        sync_id = str(uuid.uuid4())
        envelope = SyncEnvelope(
            sync_id=sync_id,
            ledgers=ledgers,
            vouchers=vouchers,
            last_ledger_sync_token=state.last_ledger_sync_token,
            last_voucher_sync_token=state.last_voucher_sync_token,
        )
        payload_json = envelope.model_dump_json()
        ciphertext = self.crypto.encrypt_text(payload_json)

        logger.info("[DEBUG] ciphertext preview: %s", ciphertext[:100])
        logger.info("[DEBUG] ciphertext length: %d", len(ciphertext))

        await self.persistence.enqueue_payload(ciphertext, sync_id=sync_id, queue_type="NORMAL")
        logger.info("[QUEUE] enqueued payload sync_id=%s queue=NORMAL", sync_id)

        await self._flush_queue_to_server(queue_type="NORMAL")

    async def _flush_queue_to_server(self, *, queue_type: str) -> bool:
        async with httpx.AsyncClient() as client:
            while True:
                item = await self.persistence.fetch_next_payload(queue_type=queue_type)
                if item is None:
                    self._send_failures = 0
                    self._next_send_time_monotonic = 0.0
                    return True
                sync_id, payload = item

                logger.info("[DEBUG] sending ciphertext preview: %s", payload[:100])
                logger.info("[DEBUG] sending ciphertext length: %d", len(payload))

                try:
                    resp = await client.post(
                        f"{self.server_base_url}/agent/sync",
                        json={"ciphertext": payload},
                        timeout=30.0,
                    )
                    resp.raise_for_status()
                    data: Optional[dict] = None
                    try:
                        data = resp.json()
                    except Exception:
                        data = None

                    if isinstance(data, dict) and data.get("status") == "RECON_REQUIRED":
                        logger.warning("[RECON] server requested full resync (RECON_REQUIRED)")
                        await self._perform_full_resync()
                        return False

                    logger.info("[FLUSH] sent payload sync_id=%s queue=%s", sync_id, queue_type)
                except Exception as exc:
                    logger.error("[ERROR] failed to push payload sync_id=%s queue=%s: %s", sync_id, queue_type, exc)
                    await self.persistence.enqueue_payload(payload, sync_id=sync_id, queue_type=queue_type)
                    self._send_failures += 1
                    backoff = min(60.0, 5.0 * (3 ** (self._send_failures - 1)))
                    self._next_send_time_monotonic = time.monotonic() + backoff
                    logger.warning("[ERROR] retrying after %.0fs (failures=%d)", backoff, self._send_failures)
                    return False

    async def _perform_full_resync(self) -> None:
        await self.persistence.set_state({"queue_status": "RECONCILING"})
        await self.persistence.clear_queue(queue_type="RECON")
        await self.persistence.clear_agent_state()
        await self.persistence.set_state({"queue_status": "RECONCILING"})

        today = date.today()
        if today.month >= 4:
            start_year = today.year
        else:
            start_year = today.year - 1
        from_date = f"{start_year}0401"
        to_date = f"{start_year + 1}0331"

        logger.info("Performing full resync for financial year %s-%s", start_year, start_year + 1)

        raw_ledgers = self.tally_client.fetch_raw_ledgers()
        raw_vouchers = self.tally_client.get_daybook(from_date, to_date)

        ledgers = extract_ledgers(raw_ledgers)
        vouchers = extract_vouchers(raw_vouchers)
        logger.info("[RECON] fetched %d vouchers for full resync", len(vouchers))

        sync_id = str(uuid.uuid4())
        envelope = SyncEnvelope(
            sync_id=sync_id,
            ledgers=ledgers,
            vouchers=vouchers,
            last_ledger_sync_token=None,
            last_voucher_sync_token=None,
        )
        payload_json = envelope.model_dump_json()
        ciphertext = self.crypto.encrypt_text(payload_json)
        await self.persistence.enqueue_payload(ciphertext, sync_id=sync_id, queue_type="RECON")
        logger.info("[QUEUE] enqueued payload sync_id=%s queue=RECON", sync_id)

    async def _run_reconciliation_once(self) -> None:
        try:
            ok = await self._flush_queue_to_server(queue_type="RECON")
            if ok:
                await self.persistence.clear_queue(queue_type="NORMAL")
                await self.persistence.set_state({"queue_status": "NORMAL"})
                logger.info("[RECON] completed successfully; resuming NORMAL mode")
        except Exception as exc:
            logger.error("Reconciliation flush encountered an unrecoverable error: %s", exc)
            await self.persistence.set_state({"queue_status": "STALE"})


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    tally_client = TallyClient()
    persistence = Persistence()
    crypto = CryptoContext.from_env_or_generate()
    logger.info("[CRYPTO] using fernet key (set AGENT_FERNET_KEY to persist across restarts)")
    worker = SyncWorker(
        tally_client=tally_client,
        persistence=persistence,
        crypto=crypto,
        server_base_url="http://localhost:8000",
    )
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())