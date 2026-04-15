import sqlite3
from pathlib import Path

# Percorso del database
DB_PATH = Path(__file__).resolve().parent.parent / "db" / "coseguardo.db"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Tabella mapping MovieLens -> IMDb -> TMDB
cur.execute("""
CREATE TABLE IF NOT EXISTS movie_links (
    movieId INTEGER PRIMARY KEY,
    imdbId TEXT,
    tmdbId INTEGER,
    mapping_source TEXT
)
""")

# Tabella dettagli film TMDB
cur.execute("""
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
    original_language TEXT
)
""")

# Tabella keywords TMDB
cur.execute("""
CREATE TABLE IF NOT EXISTS tmdb_keywords (
    tmdbId INTEGER,
    keyword_id INTEGER,
    keyword_name TEXT,
    PRIMARY KEY (tmdbId, keyword_id)
)
""")

conn.commit()
conn.close()

print("Tabelle TMDB create correttamente.")
print(f"Database usato: {DB_PATH}")