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
TTL_MEMORY_SEC  = 6 * 60 * 60        # 6h in memoria
TTL_DB_SEC      = 7 * 24 * 60 * 60   # 7 giorni in DB
MAX_MEMORY_KEYS = 1500               # cap RAM L1 (anti-OOM Render Starter 512MB)
MAX_DB_KEYS     = 30000              # cap entry DB L2 (anti-disk-full Render Starter 1GB)
DB_TRIM_CHECK_EVERY = 200            # ogni N insert, verifica se serve trim L2

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


_insert_counter = 0
_insert_counter_lock = threading.Lock()


def _db_set(key: str, value: Any, ttl: int = TTL_DB_SEC) -> None:
    global _insert_counter
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
        return  # cache failure non deve mai rompere la richiesta

    # Trim periodico: ogni N insert, controlla se siamo sopra il cap
    with _insert_counter_lock:
        _insert_counter += 1
        should_check = (_insert_counter >= DB_TRIM_CHECK_EVERY)
        if should_check:
            _insert_counter = 0

    if should_check:
        try:
            _trim_db_if_needed()
        except Exception:
            pass  # mai rompere la richiesta utente


def _trim_db_if_needed() -> int:
    """
    Se la tabella tmdb_cache supera MAX_DB_KEYS, elimina le entry più vecchie
    (per expires_at crescente = quelle che scadrebbero per prime).
    Restituisce il numero di righe eliminate.
    """
    try:
        conn = sqlite3.connect(CACHE_DB_PATH, timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM tmdb_cache")
        count = cur.fetchone()[0]
        if count <= MAX_DB_KEYS:
            conn.close()
            return 0
        # Riporta sotto il 90% del cap (lascia margine, evita re-trim immediati)
        to_delete = count - int(MAX_DB_KEYS * 0.9)
        cur.execute(
            """DELETE FROM tmdb_cache
               WHERE rowid IN (
                 SELECT rowid FROM tmdb_cache
                 ORDER BY expires_at ASC
                 LIMIT ?
               )""",
            (to_delete,)
        )
        deleted = cur.rowcount
        conn.commit()
        conn.close()
        print(f"[cache] L2 trim: count={count} → eliminate {deleted} entry più vecchie")
        return deleted
    except Exception as e:
        print(f"[cache] L2 trim ERROR: {e}")
        return 0


def cache_db_count_and_size() -> dict:
    """Statistiche tabella tmdb_cache. Per /admin/db-cache-stats."""
    _ensure_db()
    try:
        conn = sqlite3.connect(CACHE_DB_PATH, timeout=3)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*), COALESCE(SUM(length(value_json)), 0) FROM tmdb_cache")
        count, size_bytes = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM tmdb_cache WHERE expires_at < ?", (int(time.time()),))
        expired = cur.fetchone()[0]
        conn.close()
        return {
            "count": count,
            "size_bytes": size_bytes,
            "size_mb": round(size_bytes / 1024 / 1024, 1),
            "expired_count": expired,
            "max_db_keys": MAX_DB_KEYS,
            "pct_used": round(100 * count / MAX_DB_KEYS, 1) if MAX_DB_KEYS else 0,
        }
    except Exception as e:
        return {"error": str(e)}


def cache_db_trim_to(keep: int, batch_size: int = 5000, max_batches: int = 60) -> dict:
    """
    Forza trim manuale: tieni solo le `keep` entry con expires_at più alto.
    Esegue DELETE a batch (default 5000 righe per volta) per evitare timeout
    SQLite e journal enormi su disk-full.

    Restituisce dict con: deleted (int), batches (int), final_count (int),
                          partial (bool — True se ha esaurito max_batches).
    """
    _ensure_db()
    total_deleted = 0
    batches_run = 0
    partial = False
    last_error = None

    try:
        for _ in range(max_batches):
            try:
                conn = sqlite3.connect(CACHE_DB_PATH, timeout=15)
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM tmdb_cache")
                count = cur.fetchone()[0]
                if count <= keep:
                    conn.close()
                    break
                # Quante righe cancellare in questo batch
                excess = count - keep
                this_batch = min(batch_size, excess)
                cur.execute(
                    """DELETE FROM tmdb_cache
                       WHERE rowid IN (
                         SELECT rowid FROM tmdb_cache
                         ORDER BY expires_at ASC
                         LIMIT ?
                       )""",
                    (this_batch,)
                )
                deleted = cur.rowcount
                conn.commit()
                conn.close()
                total_deleted += deleted
                batches_run += 1
                if deleted == 0:
                    # Sicurezza: se per qualche motivo non cancella nulla, esci
                    break
            except Exception as e:
                last_error = str(e)
                # Salva quello che abbiamo già fatto e prova ancora una volta;
                # se fallisce di nuovo, esci (lo segnaliamo come partial)
                try:
                    conn.close()
                except Exception:
                    pass
                break
        else:
            # for-else: max_batches esaurito senza break → ancora roba da pulire
            partial = True

        # Conta finale
        try:
            conn = sqlite3.connect(CACHE_DB_PATH, timeout=5)
            final_count = conn.execute("SELECT COUNT(*) FROM tmdb_cache").fetchone()[0]
            conn.close()
        except Exception:
            final_count = -1

        result = {
            "deleted": total_deleted,
            "batches": batches_run,
            "final_count": final_count,
            "partial": partial,
        }
        if last_error:
            result["last_error"] = last_error
        return result
    except Exception as e:
        return {"deleted": total_deleted, "batches": batches_run, "error": str(e)}


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
