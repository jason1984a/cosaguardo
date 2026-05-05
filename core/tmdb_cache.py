"""
Sistema di cache TTL in-memory + DB persistente per chiamate TMDb.

Strategia:
- Cache L1 (in-memory): TTL breve (24h), velocissima, condivisa nel processo
- Cache L2 (DB SQLite): TTL lungo (7gg), persiste tra deploy e tra utenti
- Lookup order: L1 → L2 → TMDb HTTP → salva in entrambe
"""
import os
import time
import json
import sqlite3
import threading
from typing import Any, Optional, Callable

# ─── Config ──────────────────────────────────────────────────────────────
TTL_MEMORY_SEC  = 6 * 60 * 60        # 6h in memoria (era 24h — riduce footprint)
TTL_DB_SEC      = 7 * 24 * 60 * 60   # 7 giorni in DB (invariato)
MAX_MEMORY_KEYS = 1500               # cap ridotto per evitare OOM su Render Starter (era 5000)

# DB persistente: usa lo stesso DB utenti (Render persistent disk)
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_DB = os.path.join(_BASE_DIR, "app", "cosaguardo.db")
CACHE_DB_PATH = os.environ.get("DATABASE_PATH") or _DEFAULT_DB

# ─── Cache L1 in-memory ──────────────────────────────────────────────────
_mem_cache: dict[str, tuple[float, Any]] = {}
_mem_lock = threading.RLock()


def _mem_get(key: str) -> Optional[Any]:
    with _mem_lock:
        item = _mem_cache.get(key)
        if not item:
            return None
        expires_at, value = item
        if expires_at < time.time():
            _mem_cache.pop(key, None)
            return None
        return value


def _mem_set(key: str, value: Any, ttl: int = TTL_MEMORY_SEC) -> None:
    with _mem_lock:
        # Cap dimensione: se piena, rimuovi 10% più vecchi
        if len(_mem_cache) >= MAX_MEMORY_KEYS:
            cutoff = sorted(_mem_cache.items(), key=lambda kv: kv[1][0])[: MAX_MEMORY_KEYS // 10]
            for k, _ in cutoff:
                _mem_cache.pop(k, None)
        _mem_cache[key] = (time.time() + ttl, value)


# ─── Cache L2 in DB SQLite ───────────────────────────────────────────────
_db_init_done = False
_db_init_lock = threading.Lock()


def _ensure_db():
    """Crea la tabella tmdb_cache se non esiste. Idempotente."""
    global _db_init_done
    if _db_init_done:
        return
    with _db_init_lock:
        if _db_init_done:
            return
        try:
            conn = sqlite3.connect(CACHE_DB_PATH, timeout=5)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tmdb_cache (
                    cache_key   TEXT PRIMARY KEY,
                    value_json  TEXT NOT NULL,
                    expires_at  INTEGER NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tmdb_cache_expires
                ON tmdb_cache (expires_at)
            """)
            conn.commit()
            conn.close()
            _db_init_done = True
        except Exception as e:
            # Se non riesco a creare la tabella, continuo senza L2
            print(f"[cache] Impossibile creare tmdb_cache: {e}")


def _db_get(key: str) -> Optional[Any]:
    _ensure_db()
    try:
        conn = sqlite3.connect(CACHE_DB_PATH, timeout=2)
        cur = conn.cursor()
        cur.execute(
            "SELECT value_json, expires_at FROM tmdb_cache WHERE cache_key = ?",
            (key,)
        )
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        value_json, expires_at = row
        if expires_at < time.time():
            return None  # cleanup demandato a job batch
        return json.loads(value_json)
    except Exception:
        return None


def _db_set(key: str, value: Any, ttl: int = TTL_DB_SEC) -> None:
    _ensure_db()
    try:
        conn = sqlite3.connect(CACHE_DB_PATH, timeout=2)
        conn.execute(
            "INSERT OR REPLACE INTO tmdb_cache (cache_key, value_json, expires_at) VALUES (?, ?, ?)",
            (key, json.dumps(value, ensure_ascii=False), int(time.time() + ttl))
        )
        conn.commit()
        conn.close()
    except Exception:
        pass  # cache failure non deve mai rompere la richiesta


# ─── API pubblica ────────────────────────────────────────────────────────
def cache_get(key: str) -> Optional[Any]:
    """Lookup L1 → L2. Restituisce None se cache miss."""
    val = _mem_get(key)
    if val is not None:
        return val

    val = _db_get(key)
    if val is not None:
        # Promuovi in L1 per le prossime chiamate dello stesso processo
        _mem_set(key, val)
        return val
    return None


def cache_set(key: str, value: Any) -> None:
    """Salva in entrambe le cache. None viene cachato comunque (negative caching breve)."""
    _mem_set(key, value)
    # Don't cache None in DB (per evitare di persistere errori transienti TMDb)
    if value is not None:
        _db_set(key, value)


def cached_call(key: str, func: Callable[[], Any]) -> Any:
    """
    Helper: se la cache ha la risposta la restituisce, altrimenti chiama func()
    e cacha il risultato.

    Esempio:
        result = cached_call(f"tmdb_keywords:tv:{tv_id}", lambda: fetch_keywords(tv_id))
    """
    cached = cache_get(key)
    if cached is not None:
        return cached
    fresh = func()
    cache_set(key, fresh)
    return fresh


def cache_purge_expired() -> int:
    """Rimuove le entry scadute dal DB. Da chiamare in un job di pulizia."""
    _ensure_db()
    try:
        conn = sqlite3.connect(CACHE_DB_PATH, timeout=5)
        cur = conn.cursor()
        cur.execute("DELETE FROM tmdb_cache WHERE expires_at < ?", (int(time.time()),))
        n = cur.rowcount
        conn.commit()
        conn.close()
        return n
    except Exception:
        return 0


def cache_stats() -> dict:
    """Statistiche utili per debug."""
    _ensure_db()
    db_count = 0
    try:
        conn = sqlite3.connect(CACHE_DB_PATH, timeout=2)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM tmdb_cache WHERE expires_at >= ?", (int(time.time()),))
        db_count = cur.fetchone()[0]
        conn.close()
    except Exception:
        pass
    with _mem_lock:
        mem_count = len(_mem_cache)
    return {"memory": mem_count, "db": db_count}
