import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_DIR = os.path.join(BASE_DIR, "db")
DB_PATH = os.path.join(DB_DIR, "coseguardo.db")

os.makedirs(DB_DIR, exist_ok=True)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS titles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    movielens_movie_id INTEGER UNIQUE,
    title TEXT,
    year INTEGER,
    genres_raw TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS ratings (
    user_id INTEGER,
    movie_id INTEGER,
    rating REAL,
    timestamp INTEGER
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS tags (
    user_id INTEGER,
    movie_id INTEGER,
    tag TEXT,
    timestamp INTEGER
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS links (
    movie_id INTEGER,
    imdb_id TEXT,
    tmdb_id INTEGER
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS title_relations (
    source_movie_id INTEGER,
    target_movie_id INTEGER,
    relation_type TEXT,
    score_raw REAL,
    shared_users INTEGER
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS title_metrics (
    movie_id INTEGER PRIMARY KEY,
    avg_rating REAL,
    rating_count INTEGER,
    bayesian_rating REAL
)
""")

conn.commit()
conn.close()

print(f"✅ Database creato con successo: {DB_PATH}")