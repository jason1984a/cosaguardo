import os
import sqlite3
from werkzeug.security import generate_password_hash, check_password_hash

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Path del DB utenti — su Render è /data/cosaguardo.db (disco persistente),
# in locale è app/cosaguardo.db (default fallback).
# Su Render basta impostare DATABASE_PATH=/data/cosaguardo.db come env var.
DB_PATH = os.environ.get("DATABASE_PATH") or os.path.join(BASE_DIR, "cosaguardo.db")

# Crea la directory parent se non esiste (es. /data/ esiste già su Render
# ma in caso di path custom annidato lo creiamo).
_parent = os.path.dirname(DB_PATH)
if _parent and not os.path.exists(_parent):
    try:
        os.makedirs(_parent, exist_ok=True)
    except OSError:
        pass

def ensure_daily_recommendations_table():
    conn = get_connection()
    try:
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_recommendations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            rec_date TEXT NOT NULL,
            title TEXT NOT NULL,
            content_type TEXT NOT NULL,
            reason TEXT,
            score REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cursor.execute("PRAGMA table_info(daily_recommendations)")
        columns = [row[1] for row in cursor.fetchall()]

        if "poster_url" not in columns:
            cursor.execute("""
            ALTER TABLE daily_recommendations
            ADD COLUMN poster_url TEXT
            """)

        conn.commit()
    finally:
        conn.close()

def get_daily_recommendations(user_id, rec_date):
    ensure_daily_recommendations_table()

    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
        SELECT title, content_type, reason, score, poster_url
        FROM daily_recommendations
        WHERE user_id = ? AND rec_date = ?
        ORDER BY id ASC
        """, (user_id, rec_date))

        rows = cursor.fetchall()
    finally:
        conn.close()
    return rows


def save_daily_recommendations(user_id, rec_date, recommendations):
    ensure_daily_recommendations_table()

    conn = get_connection()
    try:
        cursor = conn.cursor()

        cursor.execute("""
        DELETE FROM daily_recommendations
        WHERE user_id = ? AND rec_date = ?
        """, (user_id, rec_date))

        for rec in recommendations:
            cursor.execute("""
            INSERT INTO daily_recommendations (
                user_id,
                rec_date,
                title,
                content_type,
                reason,
                score,
                poster_url
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id,
                rec_date,
                rec.get("title", ""),
                rec.get("content_type", ""),
                rec.get("reason", ""),
                rec.get("score"),
                rec.get("poster_url", ""),
            ))

        conn.commit()
    finally:
        conn.close()

def get_connection():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    # WAL mode: permette letture concorrenti senza bloccare scritture.
    # Risolve i "database is locked" da shell esterne e durante operazioni admin.
    # Idempotente: SQLite memorizza il journal_mode nel file, basta settarlo una volta.
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")  # leggermente più veloce di FULL, sicuro su WAL
        conn.execute("PRAGMA busy_timeout=5000")    # se locked, attende fino a 5s prima di errore
    except Exception:
        pass  # se PRAGMA fallisce, continua col default — mai rompere la connessione
    return conn



def get_user_by_email(email: str):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE email = ?", (email,))
        user = cur.fetchone()
    finally:
        conn.close()
    return user


def get_user_by_id(user_id: int):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        user = cur.fetchone()
    finally:
        conn.close()
    return user


def create_user(email: str, password: str,
                first_name: str = "", last_name: str = "", birth_date: str = ""):
    password_hash = generate_password_hash(password)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO users (email, password_hash, first_name, last_name, birth_date)
               VALUES (?, ?, ?, ?, ?)""",
            (email, password_hash,
             first_name.strip() or None,
             last_name.strip() or None,
             birth_date.strip() or None)
        )
        conn.commit()
        user_id = cur.lastrowid
    finally:
        conn.close()

    return user_id


def verify_user(email: str, password: str):
    user = get_user_by_email(email)
    if not user:
        return None

    if not check_password_hash(user["password_hash"], password):
        return None

    return user

def init_db():
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                first_name TEXT,
                last_name TEXT,
                birth_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Migrazione sicura — aggiunge colonne se non esistono già
        existing_cols = {row[1] for row in cur.execute("PRAGMA table_info(users)").fetchall()}
        for col, typedef in [("first_name","TEXT"), ("last_name","TEXT"), ("birth_date","TEXT")]:
            if col not in existing_cols:
                cur.execute(f"ALTER TABLE users ADD COLUMN {col} {typedef}")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS searches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                seed_titles TEXT NOT NULL,
                content_type TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                rec_date TEXT NOT NULL,
                title TEXT NOT NULL,
                content_type TEXT NOT NULL,
                reason TEXT,
                score REAL,
                poster_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                content_type TEXT NOT NULL,
                feedback_type TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, title, content_type)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_title_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                content_type TEXT NOT NULL,
                seen INTEGER NOT NULL DEFAULT 0,
                preference TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, title, content_type)
            )
        """)

        # home_picks creata qui (anche se ensure_home_picks_table la ricrea idempotentemente)
        # così l'indice qui sotto può essere creato in init_db senza dipendere da chiamate lazy.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS home_picks (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER NOT NULL,
                pick_date    TEXT NOT NULL,
                title        TEXT NOT NULL,
                content_type TEXT NOT NULL,
                reason       TEXT,
                score        REAL,
                poster_url   TEXT,
                tmdb_id      INTEGER
            )
        """)

        # ─── Indici per performance query frequenti ────────────────────────────
        # Profilo utente: SELECT ... FROM searches WHERE user_id = ? ORDER BY created_at DESC
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_searches_user_created
            ON searches (user_id, created_at DESC)
        """)

        # Profilo: SELECT ... FROM daily_recommendations WHERE user_id = ? AND rec_date = ?
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_daily_recs_user_date
            ON daily_recommendations (user_id, rec_date)
        """)

        # Home loggata: SELECT ... FROM home_picks WHERE user_id = ? AND pick_date = ?
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_home_picks_user_date
            ON home_picks (user_id, pick_date)
        """)

        # Profilo: SELECT ... FROM user_title_state WHERE user_id = ? AND content_type = ?
        # (l'UNIQUE su user_id+title+content_type non aiuta perché title è in mezzo)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_uts_user_ctype
            ON user_title_state (user_id, content_type)
        """)

        # ─── Tracking serie TV: watchlist / watching / completed ─────────────
        # Usato dalla pagina /le-mie-serie e dalle notifiche "nuova stagione".
        # Sostituisce il binario `seen` di user_title_state per le serie (i film
        # restano sul vecchio modello). PRIMARY KEY composta = upsert nativo.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_series_tracking (
                user_id               INTEGER NOT NULL,
                tmdb_id               INTEGER NOT NULL,
                title                 TEXT NOT NULL,
                poster_url            TEXT,
                status                TEXT NOT NULL,
                current_season        INTEGER,
                total_seasons_at_save INTEGER,
                created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, tmdb_id),
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        # Index per /le-mie-serie filter by status
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_series_tracking_status
            ON user_series_tracking (user_id, status, updated_at DESC)
        """)

        # ─── Cache info stagioni TMDb per detection nuove stagioni ──────────
        # Refresh lazy ogni 24h (vedi get_series_seasons_info / refresh_*).
        # status TMDb: "Returning Series" | "Ended" | "Canceled" | "In Production".
        # Detection: JOIN user_series_tracking ⋈ qui WHERE total_seasons > total_seasons_at_save.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS series_seasons_cache (
                tmdb_id        INTEGER PRIMARY KEY,
                title          TEXT,
                total_seasons  INTEGER NOT NULL DEFAULT 0,
                status         TEXT,
                last_air_date  TEXT,
                cached_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ─── Cache episodi per stagione (per sezione "Episodi" detail page) ───
        # Lista episodi per ogni stagione di una serie. Refresh lazy ogni 7 gg
        # (gli episodi futuri possono essere annunciati, i titoli cambiare).
        # episodes_json = JSON serializzato di lista di dict episodio con
        # campi: ep, title, overview, air_date, runtime, still_url.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS series_episodes_cache (
                tmdb_id        INTEGER NOT NULL,
                season_number  INTEGER NOT NULL,
                episodes_json  TEXT,
                cached_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (tmdb_id, season_number)
            )
        """)

        # ─── Streaming alerts (lead generation per titoli non disponibili) ────
        # Quando un titolo NON è ancora in streaming, l'utente lascia
        # l'email per essere avvisato quando arriva. Tabella semplice: email +
        # tmdb_id + content_type. user_id opzionale (utente loggato).
        # Future: cron job che controlla TMDb providers per ogni alert attivo,
        # quando il titolo arriva su una piattaforma manda email e marca notified.
        # Per ora: solo raccolta lead. UNIQUE(email, tmdb_id, content_type) evita
        # duplicati se lo stesso utente clicca più volte.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS streaming_alerts (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                email           TEXT NOT NULL,
                tmdb_id         INTEGER NOT NULL,
                content_type    TEXT NOT NULL,
                title           TEXT,
                user_id         INTEGER,
                notified_at     TIMESTAMP,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(email, tmdb_id, content_type)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_streaming_alerts_pending
            ON streaming_alerts (tmdb_id, content_type)
            WHERE notified_at IS NULL
        """)
        # ───────────────────────────────────────────────────────────────────────

        conn.commit()
    finally:
        conn.close()


def add_streaming_alert(email: str, tmdb_id: int, content_type: str,
                        title: str | None = None, user_id: int | None = None) -> bool:
    """
    Registra un alert per il titolo. Restituisce True se inserito, False se
    già esistente (idempotente). Usa INSERT OR IGNORE per gestire il vincolo
    UNIQUE senza sollevare eccezione.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO streaming_alerts
                (email, tmdb_id, content_type, title, user_id)
            VALUES (?, ?, ?, ?, ?)
        """, (email.strip().lower(), int(tmdb_id), content_type, title, user_id))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def count_streaming_alerts_for_title(tmdb_id: int, content_type: str) -> int:
    """Conta quante persone aspettano questo titolo (utile per social proof
    futuro tipo 'X persone in attesa'). Per ora non usata nel template ma
    pronta per quando vorrai aggiungere il counter."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM streaming_alerts
            WHERE tmdb_id = ? AND content_type = ? AND notified_at IS NULL
        """, (int(tmdb_id), content_type))
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def list_streaming_alerts(limit: int = 200, offset: int = 0) -> list[dict]:
    """
    Lista degli alert raccolti, più recenti prima. Aggregati con il titolo
    e tmdb_id così l'admin vede in un colpo d'occhio "chi ha chiesto cosa".
    Restituisce dict per essere passato direttamente al template.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, email, tmdb_id, content_type, title, user_id,
                   notified_at, created_at
            FROM streaming_alerts
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        """, (int(limit), int(offset)))
        rows = cur.fetchall()
        return [{
            "id":           r[0],
            "email":        r[1],
            "tmdb_id":      r[2],
            "content_type": r[3],
            "title":        r[4],
            "user_id":      r[5],
            "notified_at":  r[6],
            "created_at":   r[7],
        } for r in rows]
    finally:
        conn.close()


def streaming_alerts_stats() -> dict:
    """
    Statistiche aggregate sugli alert raccolti. Usato in admin per overview.
    - total_alerts: count totale di alert (incluse email duplicate cross-titoli)
    - unique_emails: count di email distinte (lead unici raccolti)
    - total_titles: count di titoli distinti per cui si aspetta uno streaming
    - pending: alert ancora da notificare (notified_at IS NULL)
    - last_7d: alert creati negli ultimi 7 giorni
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM streaming_alerts")
        total = cur.fetchone()[0] or 0
        cur.execute("SELECT COUNT(DISTINCT email) FROM streaming_alerts")
        unique_emails = cur.fetchone()[0] or 0
        cur.execute("SELECT COUNT(DISTINCT tmdb_id || '_' || content_type) FROM streaming_alerts")
        total_titles = cur.fetchone()[0] or 0
        cur.execute("SELECT COUNT(*) FROM streaming_alerts WHERE notified_at IS NULL")
        pending = cur.fetchone()[0] or 0
        cur.execute("""
            SELECT COUNT(*) FROM streaming_alerts
            WHERE created_at >= datetime('now', '-7 days')
        """)
        last_7d = cur.fetchone()[0] or 0
        return {
            "total_alerts":  int(total),
            "unique_emails": int(unique_emails),
            "total_titles":  int(total_titles),
            "pending":       int(pending),
            "last_7d":       int(last_7d),
        }
    finally:
        conn.close()


def top_requested_titles(limit: int = 10) -> list[dict]:
    """
    Top N titoli più richiesti (con più alert ancora pending). Utile per
    capire dove c'è più domanda di prodotto. Restituisce title, tmdb_id,
    content_type, alert_count ordinati per count desc.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT title, tmdb_id, content_type, COUNT(*) as alert_count
            FROM streaming_alerts
            WHERE notified_at IS NULL
            GROUP BY tmdb_id, content_type
            ORDER BY alert_count DESC, MAX(created_at) DESC
            LIMIT ?
        """, (int(limit),))
        rows = cur.fetchall()
        return [{
            "title":        r[0] or "(senza titolo)",
            "tmdb_id":      r[1],
            "content_type": r[2],
            "alert_count":  int(r[3]),
        } for r in rows]
    finally:
        conn.close()


def create_search(user_id: int, seed_titles: str, content_type: str):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO searches (user_id, seed_titles, content_type)
            VALUES (?, ?, ?)
            """,
            (user_id, seed_titles, content_type)
        )
        conn.commit()
        search_id = cur.lastrowid
    finally:
        conn.close()
    return search_id


def get_searches_by_user(user_id: int, limit: int = 10):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, user_id, seed_titles, content_type, created_at
            FROM searches
            WHERE user_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (user_id, limit)
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows

def save_feedback(user_id: int, title: str, content_type: str, feedback_type: str):
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO user_feedback (user_id, title, content_type, feedback_type)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, title, content_type)
            DO UPDATE SET feedback_type = excluded.feedback_type
            """,
            (user_id, title, content_type, feedback_type)
        )

        conn.commit()
    finally:
        conn.close()

def get_feedback_by_user(user_id: int):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT title, content_type, feedback_type
            FROM user_feedback
            WHERE user_id = ?
            """,
            (user_id,)
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def get_excluded_titles_by_user(user_id: int, content_type: str):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT title
            FROM user_feedback
            WHERE user_id = ?
              AND content_type = ?
              AND feedback_type IN ('seen', 'disliked')
            """,
            (user_id, content_type)
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return [row["title"].strip().lower() for row in rows]

def get_liked_titles_by_user(user_id: int, content_type: str | None = None):
    conn = get_connection()
    try:
        cur = conn.cursor()

        if content_type:
            cur.execute(
                """
                SELECT DISTINCT title, content_type, created_at
                FROM user_feedback
                WHERE user_id = ?
                  AND feedback_type = 'liked'
                  AND content_type = ?
                ORDER BY created_at DESC, title ASC
                """,
                (user_id, content_type)
            )
        else:
            cur.execute(
                """
                SELECT DISTINCT title, content_type, created_at
                FROM user_feedback
                WHERE user_id = ?
                  AND feedback_type = 'liked'
                ORDER BY created_at DESC, title ASC
                """,
                (user_id,)
            )

        rows = cur.fetchall()
    finally:
        conn.close()
    return rows

def upsert_title_state(user_id: int, title: str, content_type: str, seen=None, preference=None):
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT id, seen, preference
            FROM user_title_state
            WHERE user_id = ? AND title = ? AND content_type = ?
            """,
            (user_id, title, content_type)
        )
        existing = cur.fetchone()

        if existing:
            new_seen = existing["seen"] if seen is None else seen
            new_preference = existing["preference"] if preference is None else preference

            cur.execute(
                """
                UPDATE user_title_state
                SET seen = ?, preference = ?, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ? AND title = ? AND content_type = ?
                """,
                (new_seen, new_preference, user_id, title, content_type)
            )
        else:
            cur.execute(
                """
                INSERT INTO user_title_state (user_id, title, content_type, seen, preference)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    title,
                    content_type,
                    0 if seen is None else seen,
                    preference
                )
            )

        conn.commit()
    finally:
        conn.close()


def get_title_state(user_id: int, title: str, content_type: str):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT seen, preference
            FROM user_title_state
            WHERE user_id = ? AND title = ? AND content_type = ?
            """,
            (user_id, title, content_type)
        )
        row = cur.fetchone()
    finally:
        conn.close()
    return row


def get_seen_titles_by_user(user_id: int, content_type: str):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT title
            FROM user_title_state
            WHERE user_id = ?
              AND content_type = ?
              AND seen = 1
            """,
            (user_id, content_type)
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return [row["title"].strip().lower() for row in rows]


def get_seen_titles_full(user_id: int, content_type: str | None = None):
    """Variante "ricca" di get_seen_titles_by_user: ritorna dict con
    title, content_type, updated_at, ordinati per updated_at desc.
    Usata da /la-mia-raccolta per il tab "Visti" che combina film visti
    e serie completate.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        if content_type:
            cur.execute(
                """
                SELECT title, content_type, updated_at
                FROM user_title_state
                WHERE user_id = ? AND content_type = ? AND seen = 1
                ORDER BY updated_at DESC
                """,
                (user_id, content_type)
            )
        else:
            cur.execute(
                """
                SELECT title, content_type, updated_at
                FROM user_title_state
                WHERE user_id = ? AND seen = 1
                ORDER BY updated_at DESC
                """,
                (user_id,)
            )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def get_disliked_titles_by_user(user_id: int, content_type: str):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT title
            FROM user_title_state
            WHERE user_id = ?
              AND content_type = ?
              AND preference = 'disliked'
            """,
            (user_id, content_type)
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return [row["title"].strip().lower() for row in rows]


def get_liked_states_by_user(user_id: int, content_type: str | None = None):
    conn = get_connection()
    try:
        cur = conn.cursor()

        if content_type:
            cur.execute(
                """
                SELECT uts.title, uts.content_type, uts.seen, uts.preference, uts.updated_at,
                       MAX(dr.poster_url) as poster_url
                FROM user_title_state uts
                LEFT JOIN daily_recommendations dr
                    ON uts.title = dr.title
                    AND uts.content_type = dr.content_type
                    AND dr.user_id = uts.user_id
                WHERE uts.user_id = ?
                  AND uts.content_type = ?
                  AND uts.preference = 'liked'
                GROUP BY uts.title, uts.content_type
                ORDER BY uts.updated_at DESC, uts.title ASC
                """,
                (user_id, content_type)
            )
        else:
            cur.execute(
                """
                SELECT uts.title, uts.content_type, uts.seen, uts.preference, uts.updated_at,
                       MAX(dr.poster_url) as poster_url
                FROM user_title_state uts
                LEFT JOIN daily_recommendations dr
                    ON uts.title = dr.title
                    AND uts.content_type = dr.content_type
                    AND dr.user_id = uts.user_id
                WHERE uts.user_id = ?
                  AND uts.preference = 'liked'
                GROUP BY uts.title, uts.content_type
                ORDER BY uts.updated_at DESC, uts.title ASC
                """,
                (user_id,)
            )

        rows = cur.fetchall()
    finally:
        conn.close()
    return rows

def get_title_states_map(user_id: int, content_type: str):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT title, seen, preference
            FROM user_title_state
            WHERE user_id = ? AND content_type = ?
            """,
            (user_id, content_type)
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    result = {}
    for row in rows:
        key = row["title"].strip().lower()
        result[key] = {
            "seen": row["seen"],
            "preference": row["preference"],
        }

    return result


# ─── Series tracking helpers ────────────────────────────────────────────
# Tabella user_series_tracking: vedi schema in init_db().
# 3 stati ammessi (validati lato API in main.py prima di chiamare set_):
#   - 'watchlist'  voglio guardare
#   - 'watching'   sto guardando (current_season indica dove sono arrivato)
#   - 'completed'  finito
# total_seasons_at_save è uno snapshot del numero di stagioni TMDb al momento
# del set: usato in Phase 2 per detection "nuova stagione disponibile".

_SERIES_TRACKING_STATUSES = ("watchlist", "watching", "completed")

def set_series_tracking(user_id: int, tmdb_id: int, title: str,
                        status: str, current_season: int | None = None,
                        total_seasons_at_save: int | None = None,
                        poster_url: str = ""):
    """Upsert su (user_id, tmdb_id). Idempotente.
    Solleva ValueError se status non valido (linea di difesa: l'API valida già).
    """
    if status not in _SERIES_TRACKING_STATUSES:
        raise ValueError(f"status invalido: {status}")

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO user_series_tracking
                (user_id, tmdb_id, title, poster_url, status,
                 current_season, total_seasons_at_save, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id, tmdb_id) DO UPDATE SET
                title                 = excluded.title,
                poster_url            = excluded.poster_url,
                status                = excluded.status,
                current_season        = excluded.current_season,
                total_seasons_at_save = excluded.total_seasons_at_save,
                updated_at            = CURRENT_TIMESTAMP
            """,
            (user_id, tmdb_id, title, poster_url, status,
             current_season, total_seasons_at_save)
        )
        conn.commit()
    finally:
        conn.close()


def delete_series_tracking(user_id: int, tmdb_id: int) -> bool:
    """Rimuove la serie dal tracking. Ritorna True se cancellata, False se assente."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM user_series_tracking WHERE user_id = ? AND tmdb_id = ?",
            (user_id, tmdb_id)
        )
        deleted = cur.rowcount > 0
        conn.commit()
    finally:
        conn.close()
    return deleted


def get_series_tracking(user_id: int, tmdb_id: int):
    """Riga singola, None se assente. Usato dalla detail page per pre-popolare il widget."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, tmdb_id, title, poster_url, status,
                   current_season, total_seasons_at_save, updated_at
            FROM user_series_tracking
            WHERE user_id = ? AND tmdb_id = ?
            """,
            (user_id, tmdb_id)
        )
        row = cur.fetchone()
    finally:
        conn.close()
    return row


def list_series_tracking(user_id: int, status: str | None = None):
    """Lista tutte le serie tracciate, opzionalmente filtrate per status.
    Ordinate per updated_at desc (le più recenti prima).
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        if status:
            cur.execute(
                """
                SELECT tmdb_id, title, poster_url, status,
                       current_season, total_seasons_at_save, updated_at
                FROM user_series_tracking
                WHERE user_id = ? AND status = ?
                ORDER BY updated_at DESC
                """,
                (user_id, status)
            )
        else:
            cur.execute(
                """
                SELECT tmdb_id, title, poster_url, status,
                       current_season, total_seasons_at_save, updated_at
                FROM user_series_tracking
                WHERE user_id = ?
                ORDER BY updated_at DESC
                """,
                (user_id,)
            )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


# ─── series_seasons_cache helpers ───────────────────────────────────────
# Detection nuove stagioni: cache TMDb info di seasons/status/last_air_date.
# Refresh logic vive in main.py (dipende da TMDb fetch). Qui solo R/W.

def get_series_seasons_cache(tmdb_id: int):
    """Riga sqlite3.Row o None se cache miss."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT tmdb_id, title, total_seasons, status, last_air_date, cached_at
            FROM series_seasons_cache
            WHERE tmdb_id = ?
            """,
            (tmdb_id,)
        )
        row = cur.fetchone()
    finally:
        conn.close()
    return row


def get_series_seasons_cache_batch(tmdb_ids: list):
    """Lookup batch. Ritorna dict {tmdb_id: row}."""
    if not tmdb_ids:
        return {}
    conn = get_connection()
    try:
        cur = conn.cursor()
        placeholders = ",".join("?" for _ in tmdb_ids)
        cur.execute(
            f"""
            SELECT tmdb_id, title, total_seasons, status, last_air_date, cached_at
            FROM series_seasons_cache
            WHERE tmdb_id IN ({placeholders})
            """,
            tmdb_ids
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return {r["tmdb_id"]: r for r in rows}


def upsert_series_seasons_cache(tmdb_id: int, title: str = "",
                                 total_seasons: int = 0, status: str = "",
                                 last_air_date: str = ""):
    """Salva o aggiorna la cache season per una serie. cached_at = NOW.
    Idempotente. Chiamato dal refresher (sincrono o background)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO series_seasons_cache
                (tmdb_id, title, total_seasons, status, last_air_date, cached_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(tmdb_id) DO UPDATE SET
                title         = excluded.title,
                total_seasons = excluded.total_seasons,
                status        = excluded.status,
                last_air_date = excluded.last_air_date,
                cached_at     = CURRENT_TIMESTAMP
            """,
            (tmdb_id, title, total_seasons, status, last_air_date)
        )
        conn.commit()
    finally:
        conn.close()


# ─── series_episodes_cache helpers ──────────────────────────────────────
# Cache lista episodi per (tmdb_id, season_number). TTL 7 giorni.
# Restituisce dict {episodes: [...], cached_at: timestamp} se in cache valida,
# None se assente o scaduta. Il caller deve gestire il fetch + save.

def get_series_episodes_cache(tmdb_id: int, season_number: int,
                              max_age_days: int = 7) -> dict | None:
    """Restituisce dict {'episodes': list, 'cached_at': str} se in cache e
    non scaduta, None altrimenti. episodes è già deserializzato JSON→list."""
    import json
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT episodes_json, cached_at FROM series_episodes_cache
            WHERE tmdb_id = ? AND season_number = ?
              AND cached_at >= datetime('now', ?)
            """,
            (int(tmdb_id), int(season_number), f"-{int(max_age_days)} days")
        )
        row = cur.fetchone()
        if not row:
            return None
        try:
            episodes = json.loads(row[0]) if row[0] else []
        except (TypeError, ValueError):
            return None
        return {"episodes": episodes, "cached_at": row[1]}
    finally:
        conn.close()


def upsert_series_episodes_cache(tmdb_id: int, season_number: int,
                                  episodes: list):
    """Salva o aggiorna la cache episodi per (tmdb_id, season_number).
    episodes = lista di dict episodio (verrà serializzata in JSON)."""
    import json
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO series_episodes_cache
                (tmdb_id, season_number, episodes_json, cached_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(tmdb_id, season_number) DO UPDATE SET
                episodes_json = excluded.episodes_json,
                cached_at     = CURRENT_TIMESTAMP
            """,
            (int(tmdb_id), int(season_number), json.dumps(episodes, ensure_ascii=False))
        )
        conn.commit()
    finally:
        conn.close()



def ensure_home_picks_table():
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS home_picks (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER NOT NULL,
            pick_date    TEXT NOT NULL,
            title        TEXT NOT NULL,
            content_type TEXT NOT NULL,
            reason       TEXT,
            score        REAL,
            poster_url   TEXT,
            tmdb_id      INTEGER
        )
        """)
        conn.commit()
    finally:
        conn.close()


def get_home_picks(user_id: int, pick_date: str) -> list:
    """Restituisce i picks del carosello home per oggi. [] se non ancora calcolati."""
    ensure_home_picks_table()
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT title, content_type, reason, score, poster_url, tmdb_id
            FROM home_picks
            WHERE user_id = ? AND pick_date = ?
            ORDER BY id ASC
        """, (user_id, pick_date))
        rows = cursor.fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def save_home_picks(user_id: int, pick_date: str, picks: list):
    """Salva i picks del carosello home. Elimina prima quelli vecchi dello stesso giorno."""
    ensure_home_picks_table()
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM home_picks WHERE user_id = ? AND pick_date = ?",
            (user_id, pick_date)
        )
        for p in picks:
            cursor.execute("""
                INSERT INTO home_picks
                    (user_id, pick_date, title, content_type, reason, score, poster_url, tmdb_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id, pick_date,
                p.get("title", ""),
                p.get("content_type", ""),
                p.get("reason", ""),
                p.get("score"),
                p.get("poster_url", ""),
                p.get("tmdb_id"),
            ))
        conn.commit()
    finally:
        conn.close()


def get_user_stats(user_id: int) -> dict:
    """
    Calcola statistiche complete del profilo utente:
    contatori, titoli preferiti, visti, ricerche totali.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Totale ricerche
        cur.execute("SELECT COUNT(*) as cnt FROM searches WHERE user_id = ?", (user_id,))
        total_searches = cur.fetchone()["cnt"]

        # Titoli preferiti
        cur.execute("""
            SELECT title, content_type, updated_at
            FROM user_title_state
            WHERE user_id = ? AND preference = 'liked'
            ORDER BY updated_at DESC
        """, (user_id,))
        liked = [dict(r) for r in cur.fetchall()]

        # Titoli visti
        cur.execute("""
            SELECT title, content_type, updated_at
            FROM user_title_state
            WHERE user_id = ? AND seen = 1
            ORDER BY updated_at DESC
        """, (user_id,))
        seen = [dict(r) for r in cur.fetchall()]

        # Ricerche recenti (last 20 per stats generi)
        cur.execute("""
            SELECT seed_titles, content_type FROM searches
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT 20
        """, (user_id,))
        recent_searches = [dict(r) for r in cur.fetchall()]

    finally:
        conn.close()
    return {
        "total_searches": total_searches,
        "liked": liked,
        "seen": seen,
        "recent_searches": recent_searches,
        "liked_count": len(liked),
        "seen_count": len(seen),
        "movie_liked": sum(1 for x in liked if x["content_type"] == "movie"),
        "tv_liked": sum(1 for x in liked if x["content_type"] == "tv"),
    }


def save_user_onboarding(user_id: int, content_pref: str, platforms: list):
    """Salva preferenze raccolte durante la registrazione."""
    conn = get_connection()
    try:
        cursor = conn.cursor()

        # Assicura che la tabella esista
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_preferences (
                user_id      INTEGER PRIMARY KEY,
                content_pref TEXT,    -- 'movie', 'tv', 'both'
                platforms    TEXT,    -- JSON list es. '["netflix","prime"]'
                updated_at   TEXT DEFAULT (datetime('now'))
            )
        """)

        import json
        cursor.execute("""
            INSERT INTO user_preferences (user_id, content_pref, platforms)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                content_pref = excluded.content_pref,
                platforms    = excluded.platforms,
                updated_at   = datetime('now')
        """, (user_id, content_pref, json.dumps(platforms)))

        conn.commit()
    finally:
        conn.close()


def get_user_preferences(user_id: int) -> dict:
    """Recupera preferenze onboarding dell'utente."""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_preferences (
                user_id INTEGER PRIMARY KEY, content_pref TEXT,
                platforms TEXT, updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
        cursor.execute(
            "SELECT content_pref, platforms FROM user_preferences WHERE user_id = ?",
            (user_id,)
        )
        row = cursor.fetchone()
    finally:
        conn.close()
    if not row:
        return {}
    import json
    return {
        "content_pref": row["content_pref"] or "both",
        "platforms":    json.loads(row["platforms"] or "[]"),
    }


def get_admin_stats() -> dict:
    """Statistiche aggregate per la pagina admin."""
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Assicura tabelle opzionali esistano
        cur.execute("""CREATE TABLE IF NOT EXISTS user_preferences (
            user_id INTEGER PRIMARY KEY, content_pref TEXT,
            platforms TEXT, updated_at TEXT DEFAULT (datetime('now'))
        )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS searches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, seed_titles TEXT,
            content_type TEXT, created_at TEXT DEFAULT (datetime('now'))
        )""")
        cur.execute("""CREATE TABLE IF NOT EXISTS user_title_state (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, title TEXT, content_type TEXT,
            seen INTEGER DEFAULT 0, preference TEXT,
            updated_at TEXT DEFAULT (datetime('now'))
        )""")
        conn.commit()

        def count(sql, params=()):
            try:
                cur.execute(sql, params)
                row = cur.fetchone()
                return row[0] if row else 0
            except Exception:
                return 0

        total_users    = count("SELECT COUNT(*) FROM users")
        new_users_7d   = count("SELECT COUNT(*) FROM users WHERE created_at >= datetime('now', '-7 days')")
        total_searches = count("SELECT COUNT(*) FROM searches")
        total_liked    = count("SELECT COUNT(*) FROM user_title_state WHERE preference = 'liked'")
        total_seen     = count("SELECT COUNT(*) FROM user_title_state WHERE seen = 1")

        try:
            cur.execute("""
                SELECT
                    u.id, u.email, u.first_name, u.last_name,
                    u.birth_date, u.created_at,
                    COUNT(DISTINCT s.id) as n_searches,
                    COUNT(DISTINCT CASE WHEN ts.preference = 'liked' THEN ts.id END) as n_liked,
                    COUNT(DISTINCT CASE WHEN ts.seen = 1 THEN ts.id END) as n_seen,
                    p.content_pref, p.platforms
                FROM users u
                LEFT JOIN searches s ON s.user_id = u.id
                LEFT JOIN user_title_state ts ON ts.user_id = u.id
                LEFT JOIN user_preferences p ON p.user_id = u.id
                GROUP BY u.id
                ORDER BY u.created_at DESC
            """)
            users = [dict(r) for r in cur.fetchall()]
        except Exception as e:
            users = []

    finally:
        conn.close()
    return {
        "total_users":    total_users,
        "new_users_7d":   new_users_7d,
        "total_searches": total_searches,
        "total_liked":    total_liked,
        "total_seen":     total_seen,
        "users":          users,
    }


def get_poster_cache(titles_types: list) -> dict:
    """
    Recupera poster e tmdb_id dalla cache DB per una lista di (title, content_type).
    Ritorna dict: {(title, content_type): {"poster_url": ..., "tmdb_id": ...}}
    """
    if not titles_types:
        return {}
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS poster_cache (
                title        TEXT NOT NULL,
                content_type TEXT NOT NULL,
                poster_url   TEXT,
                tmdb_id      INTEGER,
                updated_at   TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (title, content_type)
            )
        """)
        conn.commit()

        placeholders = ",".join("(?,?)" for _ in titles_types)
        flat = [x for pair in titles_types for x in pair]
        cur.execute(f"""
            SELECT title, content_type, poster_url, tmdb_id
            FROM poster_cache
            WHERE (title, content_type) IN ({placeholders})
        """, flat)
        result = {}
        for row in cur.fetchall():
            result[(row["title"], row["content_type"])] = {
                "poster_url": row["poster_url"] or "",
                "tmdb_id":    row["tmdb_id"],
            }
    finally:
        conn.close()
    return result


def save_poster_cache(entries: list):
    """
    Salva poster e tmdb_id nella cache DB.
    entries: [{"title": ..., "content_type": ..., "poster_url": ..., "tmdb_id": ...}]
    """
    if not entries:
        return
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS poster_cache (
                title TEXT NOT NULL, content_type TEXT NOT NULL,
                poster_url TEXT, tmdb_id INTEGER,
                updated_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (title, content_type)
            )
        """)
        for e in entries:
            cur.execute("""
                INSERT INTO poster_cache (title, content_type, poster_url, tmdb_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(title, content_type) DO UPDATE SET
                    poster_url = excluded.poster_url,
                    tmdb_id    = excluded.tmdb_id,
                    updated_at = datetime('now')
            """, (e["title"], e["content_type"], e.get("poster_url",""), e.get("tmdb_id")))
        conn.commit()
    finally:
        conn.close()


def get_search_cache(cache_key: str) -> list | None:
    """
    Recupera risultati ricerca dalla cache DB.
    cache_key = hash(sorted seed titles + content_type).
    Valida per 24 ore.
    """
    conn = get_connection()
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS search_cache (
                cache_key  TEXT PRIMARY KEY,
                results    TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
        cur.execute("""
            SELECT results FROM search_cache
            WHERE cache_key = ?
              AND created_at >= datetime('now', '-24 hours')
        """, (cache_key,))
        row = cur.fetchone()
    finally:
        conn.close()
    if row:
        import json
        return json.loads(row["results"])
    return None


def save_search_cache(cache_key: str, results: list):
    """Salva risultati ricerca in cache DB."""
    import json
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS search_cache (
                cache_key TEXT PRIMARY KEY,
                results TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        cur.execute("""
            INSERT INTO search_cache (cache_key, results)
            VALUES (?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                results    = excluded.results,
                created_at = datetime('now')
        """, (cache_key, json.dumps(results, ensure_ascii=False)))
        conn.commit()
    finally:
        conn.close()


