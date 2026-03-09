"""
Shared utilities for ZTM Data API
"""

import os
import json
import hashlib
import time
from pathlib import Path
from datetime import datetime
from typing import Any, Optional

# ─────────────────────────────────────────
# SIMPLE FILE CACHE
# Used because Render has no Redis; resets on dyno restart
# For persistent caching, implement wp-side caching via api_cache table
# ─────────────────────────────────────────

CACHE_DIR = Path(os.getenv("CACHE_DIR", "/tmp/ztm_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def cache_key(source: str, endpoint: str, **params) -> str:
    raw = f"{source}:{endpoint}:{json.dumps(params, sort_keys=True)}"
    return hashlib.md5(raw.encode()).hexdigest()

def cache_get(key: str, ttl_seconds: int = 3600) -> Optional[Any]:
    path = CACHE_DIR / f"{key}.json"
    if not path.exists():
        return None
    age = time.time() - path.stat().st_mtime
    if age > ttl_seconds:
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None

def cache_set(key: str, data: Any) -> None:
    path = CACHE_DIR / f"{key}.json"
    try:
        path.write_text(json.dumps(data, default=str))
    except Exception:
        pass

# ─────────────────────────────────────────
# RESPONSE HELPERS
# ─────────────────────────────────────────

def ok(data: Any, source: str, cached: bool = False) -> dict:
    return {
        "status": "ok",
        "source": source,
        "cached": cached,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "data": data,
    }

