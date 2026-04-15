import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

OLD_DB = BASE_DIR / "db" / "coseguardo"
NEW_DB = BASE_DIR / "db" / "coseguardo.db"

old_conn = sqlite3.connect(OLD_DB)
old_cur = old_conn.cursor()

new_conn = sqlite3.connect(NEW_DB)
new_cur = new_conn.cursor()

new_cur.execute("""
CREATE TABLE IF NOT EXISTS movie_links (
    movieId INTEGER PRIMARY KEY,
    imdbId TEXT,
    tmdbId INTEGER,
    mapping_source TEXT
)
""")

new_cur.execute("""
CREATE TABLE IF NOT EXISTS tmdb_movies (
    tmdbId INTEGER PRIMARY KEY,
    title TEXT,
    original_title TEXT,
    overview TEXT,
    release_date TEXT,
    runtime INTEGER,
    popularity REAL,
    vote_average REAL,
    vote_count INTEGER,
    original_language TEXT,
    content_text TEXT
)
""")

new_cur.execute("""
CREATE TABLE IF NOT EXISTS tmdb_keywords (
    tmdbId INTEGER,
    keyword_id INTEGER,
    keyword_name TEXT,
    PRIMARY KEY (tmdbId, keyword_id)
)
""")

new_cur.execute("""
CREATE TABLE IF NOT EXISTS content_similarity (
    tmdbId INTEGER,
    similar_tmdbId INTEGER,
    score REAL,
    PRIMARY KEY (tmdbId, similar_tmdbId)
)
""")

new_conn.commit()

tables = ["movie_links", "tmdb_movies", "tmdb_keywords", "content_similarity"]

for table in tables:
    old_cur.execute(f"SELECT * FROM {table}")
    rows = old_cur.fetchall()

    if not rows:
        print(f"{table}: nessuna riga trovata")
        continue

    placeholders = ",".join(["?"] * len(rows[0]))
    new_cur.executemany(
        f"INSERT OR REPLACE INTO {table} VALUES ({placeholders})",
        rows
    )
    new_conn.commit()
    print(f"{table}: migrate {len(rows)} righe")

old_conn.close()
new_conn.close()

print("Migrazione completata.")