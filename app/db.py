import os
import sqlite3
from werkzeug.security import generate_password_hash, check_password_hash

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "cosaguardo.db")

def ensure_daily_recommendations_table():
    conn = get_connection()
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
    conn.close()

def get_daily_recommendations(user_id, rec_date):
    ensure_daily_recommendations_table()

    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
    SELECT title, content_type, reason, score, poster_url
    FROM daily_recommendations
    WHERE user_id = ? AND rec_date = ?
    ORDER BY id ASC
    """, (user_id, rec_date))

    rows = cursor.fetchall()
    conn.close()
    return rows


def save_daily_recommendations(user_id, rec_date, recommendations):
    ensure_daily_recommendations_table()

    conn = get_connection()
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
    conn.close()

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn



def get_user_by_email(email: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE email = ?", (email,))
    user = cur.fetchone()
    conn.close()
    return user


def get_user_by_id(user_id: int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    user = cur.fetchone()
    conn.close()
    return user


def create_user(email: str, password: str):
    password_hash = generate_password_hash(password)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO users (email, password_hash) VALUES (?, ?)",
        (email, password_hash)
    )
    conn.commit()
    user_id = cur.lastrowid
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
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

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

    conn.commit()
    conn.close()

def create_search(user_id: int, seed_titles: str, content_type: str):
    conn = get_connection()
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
    conn.close()
    return search_id


def get_searches_by_user(user_id: int, limit: int = 10):
    conn = get_connection()
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
    conn.close()
    return rows

def save_feedback(user_id: int, title: str, content_type: str, feedback_type: str):
    conn = get_connection()
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
    conn.close()

def get_feedback_by_user(user_id: int):
    conn = get_connection()
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
    conn.close()
    return rows


def get_excluded_titles_by_user(user_id: int, content_type: str):
    conn = get_connection()
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
    conn.close()
    return [row["title"].strip().lower() for row in rows]

def get_liked_titles_by_user(user_id: int, content_type: str | None = None):
    conn = get_connection()
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
    conn.close()
    return rows

def upsert_title_state(user_id: int, title: str, content_type: str, seen=None, preference=None):
    conn = get_connection()
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
    conn.close()


def get_title_state(user_id: int, title: str, content_type: str):
    conn = get_connection()
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
    conn.close()
    return row


def get_seen_titles_by_user(user_id: int, content_type: str):
    conn = get_connection()
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
    conn.close()
    return [row["title"].strip().lower() for row in rows]


def get_disliked_titles_by_user(user_id: int, content_type: str):
    conn = get_connection()
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
    conn.close()
    return [row["title"].strip().lower() for row in rows]


def get_liked_states_by_user(user_id: int, content_type: str | None = None):
    conn = get_connection()
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
    conn.close()
    return rows

def get_title_states_map(user_id: int, content_type: str):
    conn = get_connection()
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
    conn.close()

    result = {}
    for row in rows:
        key = row["title"].strip().lower()
        result[key] = {
            "seen": row["seen"],
            "preference": row["preference"],
        }

    return result



def ensure_home_picks_table():
    conn = get_connection()
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
    conn.close()


def get_home_picks(user_id: int, pick_date: str) -> list:
    """Restituisce i picks del carosello home per oggi. [] se non ancora calcolati."""
    ensure_home_picks_table()
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT title, content_type, reason, score, poster_url, tmdb_id
        FROM home_picks
        WHERE user_id = ? AND pick_date = ?
        ORDER BY id ASC
    """, (user_id, pick_date))
    rows = cursor.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def save_home_picks(user_id: int, pick_date: str, picks: list):
    """Salva i picks del carosello home. Elimina prima quelli vecchi dello stesso giorno."""
    ensure_home_picks_table()
    conn = get_connection()
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
    conn.close()


def get_user_stats(user_id: int) -> dict:
    """
    Calcola statistiche complete del profilo utente:
    contatori, titoli preferiti, visti, ricerche totali.
    """
    conn = get_connection()
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

