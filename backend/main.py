import json
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import select, or_
from fastapi.responses import FileResponse
from backend.db import get_engine, init_db, store_normalized, Voucher
from backend.config import get_database_url
from backend.crypto import CryptoContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_engine = None
_crypto: Optional[CryptoContext] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _engine, _crypto
    _engine = get_engine(get_database_url())
    init_db(_engine)
    _crypto = CryptoContext.from_env_or_generate()
    logger.info("Backend ready")
    yield
    if _engine:
        _engine.dispose()

app = FastAPI(title="Tally Sync Backend", lifespan=lifespan)
@app.get("/index.html")
async def serve_index():
    return FileResponse("index.html")

@app.get("/")
async def serve_root():
    return FileResponse("index.html")
class SyncPayload(BaseModel):
    ciphertext: str

class HeartbeatPayload(BaseModel):
    company_id: str = "default"

@app.post("/agent/sync")
async def agent_sync(body: SyncPayload):
    if _crypto is None:
        raise HTTPException(status_code=503, detail="Not ready")

    try:
        plaintext = _crypto.decrypt_text(body.ciphertext)
        envelope = json.loads(plaintext)
    except Exception as e:
        logger.error("Decryption failed: %s", e)
        logger.error("Ciphertext preview: %s", body.ciphertext[:100])
        logger.error("Ciphertext length: %d", len(body.ciphertext))
        raise HTTPException(status_code=400, detail="Invalid payload")

    vouchers = envelope.get("vouchers", [])
    sync_id = envelope.get("sync_id", "unknown")

    normalized = []
    for v in vouchers:
        ledger_entries = v.get("ledger_entries", [])
        normalized_entries = []
        for entry in ledger_entries:
            normalized_entries.append({
                "ledger_name": entry.get("ledger_name", ""),
                "amount": entry.get("amount", 0),
                "is_debit": entry.get("amount", 0) > 0,
            })
        normalized.append({
            "remote_id": v.get("external_id") or v.get("GUID") or "",
            "voucher_number": v.get("external_id", ""),
            "voucher_type": v.get("voucher_type", ""),
            "date": v.get("date", ""),
            "narration": v.get("narration", ""),
            "ledger_entries": normalized_entries
        })

    try:
        stored = store_normalized(_engine, sync_id, normalized)
        logger.info("Stored %d vouchers sync_id=%s", stored, sync_id)
    except Exception as e:
        logger.exception("Storage failed: %s", e)
        raise HTTPException(status_code=500, detail="Storage failed")

    return {"status": "OK", "stored": stored}

@app.post("/v1/sync/heartbeat")
async def heartbeat(body: HeartbeatPayload):
    return {"status": "OK", "reconciliation_required": False}

@app.get("/api/v1/query")
async def query_vouchers(q: str = ""):
    with Session(_engine) as session:
        query = select(Voucher)
        
        if q:
            search_filter = or_(
                Voucher.voucher_type.ilike(f"%{q}%"),
                Voucher.date.ilike(f"%{q}%"),
                Voucher.narration.ilike(f"%{q}%")
            )
            query = query.where(search_filter)
        
        rows = session.execute(query.order_by(Voucher.date.desc())).scalars().all()
        return {"vouchers": [
            {
                "id": v.id,
                "remote_id": v.remote_id,
                "type": v.voucher_type,
                "date": v.date,
                "narration": v.narration,
            }
            for v in rows
        ]}

@app.get("/api/v1/vouchers")
async def get_vouchers(limit: int = 100):
    with Session(_engine) as session:
        rows = session.execute(
            select(Voucher).order_by(Voucher.date.desc()).limit(limit)
        ).scalars().all()
        return {"vouchers": [
            {
                "id": v.id,
                "remote_id": v.remote_id,
                "type": v.voucher_type,
                "date": v.date,
                "narration": v.narration,
            }
            for v in rows
        ]}

@app.get("/api/v1/status")
async def status():
    return {"status": "running"}