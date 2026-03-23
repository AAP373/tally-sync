"""Backend settings from environment."""

import os

def get_database_url() -> str:
    return os.environ.get(
        "DATABASE_URL", 
        "sqlite:///./backend_state.db"
    )

def get_public_key_pem() -> bytes:
    path = os.environ.get("PUBLIC_KEY_PATH")
    if not path:
        raise RuntimeError("PUBLIC_KEY_PATH not set")
    from pathlib import Path
    return Path(path).read_bytes()