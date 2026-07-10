"""
core/push.py — Web Push per CosaGuardo (notifiche automatiche).
Autonomo (come core/seo_pages.py): tabella push_subscriptions, salvataggio/
rimozione sottoscrizioni, invio notifiche. Chiavi VAPID da env:
  VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY, VAPID_SUBJECT
Tollerante: le sottoscrizioni scadute (404/410) si rimuovono da sole; nessuna
eccezione risale al chiamante.
"""
import os, json, sqlite3, logging
log = logging.getLogger("cosaguardo")

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_DB = os.path.join(_BASE_DIR, "app", "cosaguardo.db")
PUSH_DB_PATH = os.environ.get("DATABASE_PATH") or _DEFAULT_DB

VAPID_PUBLIC_KEY  = os.environ.get("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY", "")
VAPID_SUBJECT     = os.environ.get("VAPID_SUBJECT", "mailto:privacy@cosaguardo.com")
_DB_READY = False

def _conn():
    c = sqlite3.connect(PUSH_DB_PATH, timeout=15)
    c.execute("PRAGMA journal_mode=WAL")
    return c

def init_push_db():
    global _DB_READY
    conn = _conn()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS push_subscriptions (
                endpoint TEXT PRIMARY KEY, user_id INTEGER,
                p256dh TEXT NOT NULL, auth TEXT NOT NULL, sub_json TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')), last_ok TEXT
            )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_push_user ON push_subscriptions(user_id)")
        conn.commit(); _DB_READY = True
    finally:
        conn.close()

def _ensure():
    if not _DB_READY: init_push_db()

def is_configured() -> bool:
    return bool(VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY)

def save_subscription(subscription: dict, user_id=None) -> bool:
    _ensure()
    try:
        endpoint = subscription.get("endpoint"); keys = subscription.get("keys") or {}
        p256dh = keys.get("p256dh"); auth = keys.get("auth")
        if not endpoint or not p256dh or not auth: return False
        conn = _conn()
        try:
            conn.execute("""
                INSERT INTO push_subscriptions (endpoint, user_id, p256dh, auth, sub_json)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(endpoint) DO UPDATE SET
                    user_id=excluded.user_id, p256dh=excluded.p256dh,
                    auth=excluded.auth, sub_json=excluded.sub_json
            """, (endpoint, user_id, p256dh, auth, json.dumps(subscription)))
            conn.commit()
        finally:
            conn.close()
        return True
    except Exception as e:
        log.warning("push save_subscription fallito: %s", e); return False

def delete_subscription(endpoint: str):
    _ensure()
    try:
        conn = _conn()
        try:
            conn.execute("DELETE FROM push_subscriptions WHERE endpoint = ?", (endpoint,)); conn.commit()
        finally:
            conn.close()
    except Exception as e:
        log.warning("push delete_subscription fallito: %s", e)

def get_subscriptions_for_user(user_id):
    _ensure()
    conn = _conn()
    try:
        rows = conn.execute("SELECT sub_json FROM push_subscriptions WHERE user_id = ?", (user_id,)).fetchall()
        return [json.loads(r[0]) for r in rows]
    finally:
        conn.close()

def send_push_to_user(user_id, title, body, url="/", tag=None, icon=None) -> int:
    if not is_configured():
        log.info("push non configurato (VAPID mancanti) — invio saltato"); return 0
    _ensure()
    from pywebpush import webpush, WebPushException
    payload = json.dumps({"title": title, "body": body, "url": url,
                          "tag": tag or "cosaguardo", "icon": icon or "/static/icons/icon-192.png"})
    sent = 0
    for sub in get_subscriptions_for_user(user_id):
        endpoint = sub.get("endpoint", "")
        try:
            webpush(subscription_info=sub, data=payload, vapid_private_key=VAPID_PRIVATE_KEY,
                    vapid_claims={"sub": VAPID_SUBJECT}, ttl=86400)
            sent += 1
        except WebPushException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in (404, 410):
                delete_subscription(endpoint); log.info("push: rimossa sottoscrizione scaduta (%s)", status)
            else:
                log.warning("push invio fallito (%s): %s", status, e)
        except Exception as e:
            log.warning("push invio errore generico: %s", e)
    return sent
