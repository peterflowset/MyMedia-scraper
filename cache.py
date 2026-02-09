from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
from pathlib import Path

from models import Business

logger = logging.getLogger(__name__)

_DB_PATH = Path("cache.db")
_TTL_SECONDS = 7 * 24 * 3600  # 7 days


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_DB_PATH))
    conn.execute(
        """CREATE TABLE IF NOT EXISTS cache (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            created_at REAL NOT NULL
        )"""
    )
    return conn


def _cache_key(*parts: str) -> str:
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()


def get_cached_businesses(
    business_type: str, city: str, country: str, limit: int
) -> list[Business] | None:
    """Return cached OutScraper results if fresh enough, else None."""
    key = _cache_key("outscraper", business_type, city, country, str(limit))
    try:
        conn = _get_conn()
        row = conn.execute(
            "SELECT value, created_at FROM cache WHERE key = ?", (key,)
        ).fetchone()
        conn.close()
        if row is None:
            return None
        value, created_at = row
        if time.time() - created_at > _TTL_SECONDS:
            return None
        data = json.loads(value)
        return [Business(**b) for b in data]
    except Exception as e:
        logger.debug(f"Cache-Lesefehler: {e}")
        return None


def set_cached_businesses(
    business_type: str, city: str, country: str, limit: int, businesses: list[Business]
) -> None:
    """Store OutScraper results in cache."""
    key = _cache_key("outscraper", business_type, city, country, str(limit))
    try:
        value = json.dumps([b.model_dump() for b in businesses], ensure_ascii=False)
        conn = _get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO cache (key, value, created_at) VALUES (?, ?, ?)",
            (key, value, time.time()),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"Cache-Schreibfehler: {e}")


def get_cached_enrichment(business_name: str, website: str) -> Business | None:
    """Return cached enrichment result for a business."""
    key = _cache_key("enrichment", business_name, website)
    try:
        conn = _get_conn()
        row = conn.execute(
            "SELECT value, created_at FROM cache WHERE key = ?", (key,)
        ).fetchone()
        conn.close()
        if row is None:
            return None
        value, created_at = row
        if time.time() - created_at > _TTL_SECONDS:
            return None
        return Business(**json.loads(value))
    except Exception as e:
        logger.debug(f"Cache-Lesefehler (Enrichment): {e}")
        return None


def set_cached_enrichment(business: Business) -> None:
    """Store enriched business in cache."""
    key = _cache_key("enrichment", business.name, business.website)
    try:
        value = json.dumps(business.model_dump(), ensure_ascii=False)
        conn = _get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO cache (key, value, created_at) VALUES (?, ?, ?)",
            (key, value, time.time()),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"Cache-Schreibfehler (Enrichment): {e}")
