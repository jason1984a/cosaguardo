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